import asyncio
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


INDEX_URL = "https://spa5.scrape.center/api/book/?limit={limit}&offset={offset}"
DETAIL_URL = "https://spa5.scrape.center/api/book/{id}/"
PAGE_SIZE = 18
PAGE_NUMBER = 2
CONCURRENCY = 5

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "spider_center"
MONGO_COLLECTION = "spa5_books1"


async def scrape_api(url, session, semaphore):
    async with semaphore:
        try:
            logging.info("scrape %s", url)
            async with session.get(url=url) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
            logging.error("error: %s occurred while scraping %s", error, url)
            return None


async def scrape_index(page, session, semaphore):
    offset = PAGE_SIZE * (page - 1)
    url = INDEX_URL.format(limit=PAGE_SIZE, offset=offset)
    return await scrape_api(url, session, semaphore)


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
    data = await scrape_api(url, session, semaphore)

    if not isinstance(data, dict):
        stats["detail_failed"] += 1
        return

    stats["detail_success"] += 1
    if await save_data(data, collection):
        stats["saved"] += 1
    else:
        stats["save_failed"] += 1


async def main():
    started_at =    time.perf_counter()
    stats = {
        "index_success": 0,
        "index_failed": 0,
        "detail_success": 0,
        "detail_failed": 0,
        "saved": 0,
        "save_failed": 0,
    }

    client = AsyncIOMotorClient(MONGO_URI)

    try:
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]

        semaphore = asyncio.Semaphore(CONCURRENCY)

        async with aiohttp.ClientSession() as session:
            index_tasks = [
                scrape_index(page, session, semaphore) for page in range(1, PAGE_NUMBER + 1)
            ]
            index_results = await asyncio.gather(*index_tasks)

            books_ids = set()
            for result in index_results:
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
            await asyncio.gather(*detail_tasks)
    finally:
        client.close()
        elapsed = time.perf_counter() - started_at
        logging.info(
            "summary | elapsed=%.2fs | index_success=%d | index_failed=%d | "
            "detail_success=%d | detail_failed=%d | saved=%d | save_failed=%d",
            elapsed,
            stats["index_success"],
            stats["index_failed"],
            stats["detail_success"],
            stats["detail_failed"],
            stats["saved"],
            stats["save_failed"],
        )

if __name__ == "__main__":
    asyncio.run(main())

