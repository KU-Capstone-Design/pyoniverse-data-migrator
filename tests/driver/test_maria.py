import os

from mylib.driver.maria import MariaDriver
from tests.conftest import not_raises


def test_read(maria_client):
    driver = MariaDriver(client=maria_client)
    data = driver.read("categories", 5)
    data = list(data)
    assert len(data) == 5


def test_write(maria_client):
    driver = MariaDriver(client=maria_client)
    data = [{"id": 1, "name": "test3"}]
    with not_raises():
        driver.write("tests", data)
