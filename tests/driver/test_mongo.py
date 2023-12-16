import os
from typing import Iterable

from mylib.driver.mongo import MongoDriver


def test_read(mongo_client):
    driver = MongoDriver(client=mongo_client)
    data = driver.read(os.getenv("MONGO_DB"), "products", 100)
    data = list(data)
    assert len(data) == 100
    assert sorted(data, key=lambda x: x["id"]) == data
