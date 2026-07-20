import os
from dotenv import load_dotenv

load_dotenv()

INDEX_URL = "https://spa5.scrape.center/api/book/?limit={limit}&offset={offset}"
DETAIL_URL = "https://spa5.scrape.center/api/book/{id}/"

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "18"))
PAGE_NUMBER = int(os.getenv("PAGE_NUMBER", "2"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
DETAIL_QUEUE_SIZE = int(os.getenv("DETAIL_QUEUE_SIZE", str(CONCURRENCY * 2)))

REQUEST_TOTAL_TIMEOUT = float(os.getenv("REQUEST_TOTAL_TIMEOUT", "20"))
REQUEST_CONNECT_TIMEOUT = float(os.getenv("REQUEST_CONNECT_TIMEOUT", "5"))
REQUEST_READ_TIMEOUT = float(os.getenv("REQUEST_READ_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1"))

REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": os.getenv("USER_AGENT", "async-books-crawler/2.0"),
}

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "spider_center")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "spa5_books1")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

if PAGE_SIZE < 1 or PAGE_NUMBER < 1 or CONCURRENCY < 1:
    raise ValueError("PAGE_SIZE, PAGE_NUMBER and CONCURRENCY must be positive")

if DETAIL_QUEUE_SIZE < 1:
    raise ValueError("DETAIL_QUEUE_SIZE must be positive")

if (
    REQUEST_TOTAL_TIMEOUT <= 0
    or REQUEST_CONNECT_TIMEOUT <= 0
    or REQUEST_READ_TIMEOUT <= 0
):
    raise ValueError("request timeouts must be positive")

if MAX_RETRIES < 0:
    raise ValueError("MAX_RETRIES cannot be negative")

if BACKOFF_BASE < 0:
    raise ValueError("BACKOFF_BASE cannot be negative")
