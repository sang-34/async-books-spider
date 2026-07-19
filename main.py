import asyncio
import json
import logging

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


INDEX_URL = "https://spa5.scrape.center/api/book/?limit=18&offset={offset}"
DETAIL_URL = "https://spa5.scrape.center/api/book/{id}/"
PAGE_SIZE = 18
PAGE_NUMBER = 2
CONCURRENCY = 5

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "spider_center"
MONGO_COLLECTION = "spa5_books1"

client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]


async def scrape_api(url, session: aiohttp.ClientSession, semaphore):
    async with semaphore:
        try:
            logging.info("scrape %s", url)
            async with session.get(url=url) as response:
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error("error: %s occurred while scraping %s", e, url)

async def scrape_index(page, session: aiohttp.ClientSession, semaphore):
    url = INDEX_URL.format(offset=18 * (page - 1))
    return await scrape_api(url, session, semaphore)

async def scrape_detail(book_id, session: aiohttp.ClientSession, semaphore):
    url = DETAIL_URL.format(id=book_id)
    data = await scrape_api(url, session, semaphore)
    await save_data(data)

async def save_data(data):
    if data:
        book_id = data.get("id")
        logging.info("save data id: %s", book_id)
        await collection.update_one(
            {"id": book_id},
            {"$set": data},
            upsert=True,
        )

async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        scrape_index_tasks = [
            scrape_index(page, session, semaphore) for page in range(1, PAGE_NUMBER + 1)
        ]
        results = await asyncio.gather(*scrape_index_tasks)
        logging.info(
            "results: %s",
            json.dumps(results, indent=2, ensure_ascii=False)
        )

        ids = []
        for result in results:
            if not result or "results" not in result:
                continue

            for item in result["results"]:
                ids.append(item.get("id"))

        scrape_detail_tasks = [scrape_detail(book_id, session, semaphore) for book_id in ids]
        await asyncio.gather(*scrape_detail_tasks)

        client.close()

if __name__ == "__main__":
    asyncio.run(main())

