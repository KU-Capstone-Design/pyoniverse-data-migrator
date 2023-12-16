import os

from mylib.driver.mongo import MongoDriver
from tests.conftest import not_raises


def test_read(mongo_client):
    driver = MongoDriver(client=mongo_client)
    data = driver.read(os.getenv("MONGO_DB"), "products", 100)
    data = list(data)
    assert len(data) == 100
    assert sorted(data, key=lambda x: x["id"]) == data


def test_write(mongo_client):
    driver = MongoDriver(client=mongo_client)
    data = driver.read(os.getenv("MONGO_DB"), "products", 100)
    with not_raises():
        driver.write(os.getenv("MONGO_DB"), "products", data)
