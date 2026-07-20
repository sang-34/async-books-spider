import asyncio
import logging
import time

import aiohttp

from config import (
    CONCURRENCY, DETAIL_QUEUE_SIZE, LOG_FORMAT, LOG_LEVEL,
    MONGO_COLLECTION, MONGO_DATABASE, MONGO_URI,
    PAGE_NUMBER, REQUEST_CONNECT_TIMEOUT, REQUEST_HEADERS,
    REQUEST_READ_TIMEOUT, REQUEST_TOTAL_TIMEOUT,
)
from crawler import BookCrawler
from storage import MongoStorage

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FORMAT,
)

logger = logging.getLogger(__name__)
WORKER_STOP = object()


def create_stats():
    return {
        "index_success": 0,
        "index_failed": 0,
        "detail_success": 0,
        "detail_failed": 0,
        "saved": 0,
        "save_failed": 0,
        "request_retries": 0,
        "retry_exhausted": 0,
        "queued": 0,
        "worker_errors": 0,
    }


async def collect_book_ids(crawler, stats):
    index_tasks = [
        crawler.scrape_index(page) for page in range(1, PAGE_NUMBER + 1)
    ]

    index_results = await asyncio.gather(*index_tasks, return_exceptions=True)

    book_ids = set()

    for result in index_results:
        if isinstance(result, Exception):
            stats["index_failed"] += 1
            logger.error("index task failed unexpectedly: %r", result)
            continue

        if not isinstance(result, dict):
            stats["index_failed"] += 1
            logger.warning("index response is not a dictionary")
            continue

        items = result.get("results")
        if not isinstance(items, list):
            stats["index_failed"] += 1
            logger.warning("index response is missing a valid results list")
            continue

        stats["index_success"] += 1

        items = result.get("results")
        if not isinstance(items, list):
            stats["index_failed"] += 1
            logger.warning("index response is missing a valid results list")
            continue

        stats["index_success"] += 1

        for item in items:
            if not isinstance(item, dict):
                continue

            book_id = item.get("id")
            if book_id is not None:
                book_ids.add(book_id)

    logger.info("collected %d unique book ids", len(book_ids))
    return book_ids


async def detail_worker(worker_id, queue, crawler, storage, stats):
    logging.info("detail worker %d started", worker_id)

    while True:
        book_id = await queue.get()

        try:
            if book_id is WORKER_STOP:
                logging.info("detail worker %d stopped", worker_id)
                return

            data = await crawler.scrape_detail(book_id)

            if not isinstance(data, dict):
                stats["detail_failed"] += 1
                continue

            if await storage.save_data(data):
                stats["detail_success"] += 1
                stats["saved"] += 1
            else:
                stats["detail_failed"] += 1
                stats["save_failed"] += 1

        except Exception:
            stats["detail_failed"] += 1
            stats["worker_errors"] += 1
            logger.exception(
                "detail worker %d failed while processing book id=%s",
                worker_id, book_id,
            )
        finally:
            queue.task_done()


async def process_detail_queue(books_ids, crawler, storage, stats):
    queue = asyncio.Queue(maxsize=DETAIL_QUEUE_SIZE)
    stats["queued"] = len(books_ids)

    workers = [
        asyncio.create_task(
            detail_worker(worker_id, queue, crawler, storage, stats),
            name=f"detail-worker-{worker_id}",
        )
        for worker_id in range(1, CONCURRENCY + 1)
    ]

    try:
        for book_id in books_ids:
            await queue.put(book_id)

        for _ in workers:
            await queue.put(WORKER_STOP)

        await queue.join()
    finally:
        for worker in workers:
            if not worker.done():
                worker.cancel()

        workers_result = await asyncio.gather(*workers, return_exceptions=True)

        for result in workers_result:
            if isinstance(result, Exception):
                stats["worker_errors"] += 1
                logging.error("detail worker exited unexpectedly: %r",result)

    logging.info(
        "detail queue drained queued=%d remaining=%d workers=%d",
        stats["queued"], queue.qsize(), len(workers),
    )


async def main():
    started_at = time.perf_counter()
    stats = create_stats()

    storage = MongoStorage(MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION)

    try:
        if not await storage.initialize():
            raise RuntimeError("MongoDB initialization failed")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        timeout = aiohttp.ClientTimeout(
            total=REQUEST_TOTAL_TIMEOUT,
            connect=REQUEST_CONNECT_TIMEOUT,
            sock_read=REQUEST_READ_TIMEOUT,
        )

        async with aiohttp.ClientSession(
            timeout=timeout, headers=REQUEST_HEADERS
        ) as session:
            crawler = BookCrawler(session, semaphore, stats)

            book_ids = await collect_book_ids(crawler, stats)

            await process_detail_queue(book_ids, crawler, storage, stats)
    finally:
        storage.close()
        
        elapsed = time.perf_counter() - started_at
        logging.info(
            "summary | elapsed=%.2fs | index_success=%d | index_failed=%d | "
            "detail_success=%d | detail_failed=%d | saved=%d | save_failed=%d "
            "| request_retries=%d | retry_exhausted=%d | queued=%d | worker_errors=%d ",
            elapsed,
            stats["index_success"], stats["index_failed"], stats["detail_success"],
            stats["detail_failed"], stats["saved"], stats["save_failed"],
            stats["request_retries"], stats["retry_exhausted"],stats["queued"],
            stats["worker_errors"],
        )

if __name__ == "__main__":
    asyncio.run(main())

