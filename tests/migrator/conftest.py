import pytest

from mylib.driver.maria import MariaDriver
from mylib.driver.mongo import MongoDriver


@pytest.fixture(scope="session")
def maria_driver(maria_client):
    return MariaDriver(client=maria_client)


@pytest.fixture(scope="session")
def mongo_driver(mongo_client):
    return MongoDriver(client=mongo_client)
