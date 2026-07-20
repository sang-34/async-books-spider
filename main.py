import asyncio
import json
import logging
import time

import aiohttp
from bson.errors import BSONError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=20,
    connect=5,
    sock_read=10,
)

REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "async-books-crawler/2.0",
}

MAX_RETRIES = 3
BACKOFF_BASE = 1.0

INDEX_URL = "https://spa5.scrape.center/api/book/?limit={limit}&offset={offset}"
DETAIL_URL = "https://spa5.scrape.center/api/book/{id}/"
PAGE_SIZE = 18
PAGE_NUMBER = 2
CONCURRENCY = 5

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "spider_center"
MONGO_COLLECTION = "spa5_books1"


async def scrape_api(url, session, semaphore, stats):
    total_attempts = MAX_RETRIES + 1

    for attempt in range(total_attempts):
        attempt_number = attempt + 1
        retry_reason = None

        try:
            async with semaphore:
                logging.info(
                    "scrape %s, attempt=%d/%d",
                    url, attempt_number, total_attempts
                )

                async with session.get(url=url) as response:
                    status = response.status

                    if status == 200:
                        try:
                            return await response.json()
                        except aiohttp.ContentTypeError as error:
                            retry_reason = "invalid content type"
                            logging.warning(
                                "JSON content-type error url=%s error=%s",
                                url, error,
                            )
                        except (json.JSONDecodeError, ValueError) as error:
                            retry_reason = "invalid JSON"
                            logging.warning(
                                "JSON decode error url=%s error=%s",
                                url, error,
                            )

                    elif status == 429:
                        retry_reason = "HTTP 429"
                        logging.warning("rate limited status=429 url=%s", url)

                    elif 500 <= status <= 599:
                        retry_reason = f"HTTP {status}"
                        logging.warning(
                            "server error status=%d url=%s",status, url)

                    elif 400 <= status <= 499:
                        logging.error(
                            "non-retryable client error status=%d url=%s",
                            status, url
                        )
                        return None

                    else:
                        logging.error(
                            "unexpected HTTP status=%d url=%s",status, url)
                        return None

        except asyncio.TimeoutError as error:
            retry_reason = "timeout"
            logging.warning("request timeout url=%s error=%s", url, error)

        except aiohttp.ClientError as error:
            retry_reason = "client error"
            logging.warning("HTTP client error url=%s error=%s", url, error)

        if attempt >= MAX_RETRIES:
            stats["retry_exhausted"] += 1
            logging.error(
                "request failed after %d attempts url=%s reason=%s",
                total_attempts, url, retry_reason
            )
            return None

        delay = BACKOFF_BASE * (2 ** attempt)
        stats["request_retries"] += 1
        logging.info(
            "retrying url=%s reason=%s wait=%.1fs",
            url, retry_reason, delay
        )
        await asyncio.sleep(delay)

    return None


async def scrape_index(page, session, semaphore, stats):
    offset = PAGE_SIZE * (page - 1)
    url = INDEX_URL.format(limit=PAGE_SIZE, offset=offset)
    return await scrape_api(url, session, semaphore, stats)


async def save_data(data, collection):
    book_id = data.get("id")
    if book_id is None:
        logging.info("skip data without id")
        return False

    try:
        await collection.update_one(
            {"id": book_id},
            {"$set": data},
            upsert=True,
        )
        logging.info("save data id: %s", book_id)
        return True
    except (PyMongoError, BSONError):
        logging.error("failed to save data id: %s", book_id)
        return False


async def scrape_detail(book_id, session, semaphore, collection, stats):
    url = DETAIL_URL.format(id=book_id)
    data = await scrape_api(url, session, semaphore, stats)

    if not isinstance(data, dict):
        stats["detail_failed"] += 1
        return

    stats["detail_success"] += 1
    if await save_data(data, collection):
        stats["saved"] += 1
    else:
        stats["save_failed"] += 1


async def main():
    started_at = time.perf_counter()
    stats = {
        "index_success": 0,
        "index_failed": 0,
        "detail_success": 0,
        "detail_failed": 0,
        "saved": 0,
        "save_failed": 0,
        "request_retries": 0,
        "retry_exhausted": 0,
    }

    client = AsyncIOMotorClient(MONGO_URI)

    try:
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]

        semaphore = asyncio.Semaphore(CONCURRENCY)

        async with aiohttp.ClientSession(
            timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS
        ) as session:
            index_tasks = [
                scrape_index(page, session, semaphore, stats)
                for page in range(1, PAGE_NUMBER + 1)
            ]
            index_results = await asyncio.gather(*index_tasks, return_exceptions=True)

            books_ids = set()
            for result in index_results:
                if isinstance(result, Exception):
                    stats["index_failed"] += 1
                    logging.error("index task failed unexpectedly: %r", result)
                    continue

                if not isinstance(result, dict) or not isinstance(result.get("results"), list):
                    stats["index_failed"] += 1
                    logging.warning("index response is missing a valid results list")
                    continue

                stats["index_success"] += 1
                for item in result["results"]:
                    if not isinstance(item, dict):
                        continue
                    book_id = item.get("id")
                    if book_id is not None:
                        books_ids.add(book_id)

            logging.info("collected %d unique books ids", len(books_ids))

            detail_tasks = [
                scrape_detail(book_id, session, semaphore, collection, stats)
                for book_id in books_ids
            ]
            detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

            for result in detail_results:
                if isinstance(result, Exception):
                    stats["detail_failed"] += 1
                    logging.error("detail task failed unexpectedly: %r", result)

    finally:
        client.close()
        elapsed = time.perf_counter() - started_at
        logging.info(
            "summary | elapsed=%.2fs | index_success=%d | index_failed=%d | "
            "detail_success=%d | detail_failed=%d | saved=%d | save_failed=%d "
            "| request_retries=%d | retry_exhausted=%d ",
            elapsed,
            stats["index_success"], stats["index_failed"], stats["detail_success"],
            stats["detail_failed"], stats["saved"], stats["save_failed"],
            stats["request_retries"], stats["retry_exhausted"],
        )

if __name__ == "__main__":
    asyncio.run(main())

