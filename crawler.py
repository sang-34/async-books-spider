import asyncio
import json
import logging

import aiohttp

from config import (
    MAX_RETRIES, BACKOFF_BASE, PAGE_SIZE,
    INDEX_URL, DETAIL_URL
)


class BookCrawler:
    def __init__(self, session, semaphore, stats):
        self.session = session
        self.semaphore = semaphore
        self.stats = stats

    async def request(self, url):
        total_attempts = MAX_RETRIES + 1

        for attempt in range(total_attempts):
            attempt_number = attempt + 1
            retry_reason = None

            try:
                async with self.semaphore:
                    logging.info(
                        "scrape %s, attempt=%d/%d",
                        url, attempt_number, total_attempts
                    )

                    async with self.session.get(url=url) as response:
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
                                "server error status=%d url=%s", status, url)

                        elif 400 <= status <= 499:
                            logging.error(
                                "non-retryable client error status=%d url=%s",
                                status, url
                            )
                            return None

                        else:
                            logging.error(
                                "unexpected HTTP status=%d url=%s", status, url)
                            return None

            except asyncio.TimeoutError as error:
                retry_reason = "timeout"
                logging.warning("request timeout url=%s error=%s", url, error)

            except aiohttp.ClientError as error:
                retry_reason = "client error"
                logging.warning("HTTP client error url=%s error=%s", url, error)

            if attempt >= MAX_RETRIES:
                self.stats["retry_exhausted"] += 1
                logging.error(
                    "request failed after %d attempts url=%s reason=%s",
                    total_attempts, url, retry_reason
                )
                return None

            delay = BACKOFF_BASE * (2 ** attempt)
            self.stats["request_retries"] += 1
            logging.info(
                "retrying url=%s reason=%s wait=%.1fs",
                url, retry_reason, delay
            )
            await asyncio.sleep(delay)

        return None

    async def scrape_index(self, page):
        offset = PAGE_SIZE * (page - 1)
        url = INDEX_URL.format(limit=PAGE_SIZE, offset=offset)
        return await self.request(url)

    async def scrape_detail(self, book_id):
        return await self.request(DETAIL_URL.format(id=book_id))



