import logging
import os
from contextlib import contextmanager

import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


@pytest.fixture(scope="session")
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


@pytest.fixture(scope="session")
def mongo_client(env):
    client = MongoClient(os.getenv("MONGO_URI"))
    try:
        # The ping command is cheap and does not require auth.
        client.admin.command("ping")
    except ConnectionFailure:
        logging.error("Server not available")
    yield client
    client.close()


@pytest.fixture(scope="session")
def maria_client(env):
    import pymysql
    from pymysql.cursors import DictCursor

    client = pymysql.connect(
        host=os.getenv("MARIA_HOST"),
        port=int(os.getenv("MARIA_PORT")),
        user=os.getenv("MARIA_USER"),
        password=os.getenv("MARIA_PASSWORD"),
        database=os.getenv("MARIA_DB"),
        cursorclass=DictCursor,
    )
    yield client
    client.close()


@contextmanager
def not_raises():
    try:
        yield
    except Exception as e:
        logging.error(e)
        assert False
    else:
        assert True
