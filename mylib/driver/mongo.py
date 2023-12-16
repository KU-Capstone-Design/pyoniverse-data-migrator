from typing import Iterable

from pymongo import MongoClient, ReadPreference

from mylib.interface import Driver


class MongoDriver(Driver):
    def __init__(self, client: MongoClient):
        self.__client = client

    def read(self, db: str, rel: str, n: int) -> Iterable[dict]:
        db = self.__client.get_database(db, read_preference=ReadPreference.SECONDARY_PREFERRED)
        coll = db.get_collection(rel)
        data = coll.find().sort("id", 1).limit(n)
        return data

    def write(self, db: str, rel: str, updated: Iterable[dict]) -> None:
        pass
