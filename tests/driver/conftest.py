import logging
import os

import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


@pytest.fixture
def mongo_client(env):
    client = MongoClient(os.getenv("MONGO_URI"))
    try:
        # The ping command is cheap and does not require auth.
        client.admin.command("ping")
    except ConnectionFailure:
        logging.error("Server not available")
    return client
