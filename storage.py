import logging
from datetime import datetime, timezone

from bson.errors import BSONError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError


class MongoStorage:

    def __init__(self, uri, database, collection):
        self.client = AsyncIOMotorClient(uri)
        self.collection = self.client[database][collection]

    async def initialize(self):
        try:
            await self.collection.create_index(
                "id",
                unique=True,
                name="unique_book_id",
            )

            now = datetime.now(timezone.utc)

            await self.collection.update_many(
                {"created_at": {"$exists": False}},
                {"$set": {"created_at": now}},
            )
            await self.collection.update_many(
                {"updated_at": {"$exists": False}},
                {"$set": {"updated_at": now}},
            )

            logging.info("MongoDB unique id index is ready")
            return True
        except PyMongoError:
            logging.exception("failed to initialize MongoDB indexes")
            return False

    async def save_data(self, data):
        if not isinstance(data, dict):
            logging.error("cannot save non-dict data")
            return False

        book_id = data.get("id")
        if book_id is None:
            logging.error("cannot save data without id")
            return False

        document = dict(data)
        document.pop("_id", None)
        document.pop("created_at", None)
        document.pop("updated_at", None)

        now = datetime.now(timezone.utc)

        try:
            await self.collection.update_one(
                {"id": book_id},
                {
                    "$set": {
                        **document,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "created_at": now,
                    },
                },
                upsert=True,
            )
            logging.info("save data id: %s", book_id)
            return True
        except (PyMongoError, BSONError):
            logging.error("failed to save data id: %s", book_id)
            return False

    def close(self):
        self.client.close()
        logging.info("MongoDB client closed")




