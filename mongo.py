from datetime import datetime, date
from pymongo import MongoClient

from config import config

client = MongoClient(
    f"mongodb://{config.mongo.user}:{config.mongo.password}@{config.mongo.host}:{config.mongo.port}",
    document_class=dict,
    tz_aware=False,
    connect=True,
)


class Articles:
    @staticmethod
    def transcripts(tickers, before=None, after=None):

        if before and isinstance(before, date):
            before = datetime(before.year, before.month, before.day)
        if after and isinstance(after, date):
            after = datetime(after.year, after.month, after.day)

        query = {"tags": "earnings call", "tags": {"$in": tickers}}

        if before:
            if "date" not in query:
                query["date"] = {}
            query["date"]["$lt"] = (
                datetime.fromisoformat(before) if isinstance(before, str) else before
            )
        if after:
            if "date" not in query:
                query["date"] = {}
            query["date"]["$gte"] = (
                datetime.fromisoformat(after) if isinstance(after, str) else after
            )

        collection = client[config.mongo.db]["articles"]

        return list(collection.find(query))
