import logging
from contextlib import contextmanager

import pytest


@pytest.fixture(scope="session")
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


@contextmanager
def not_raises():
    try:
        yield
    except Exception as e:
        logging.error(e)
        assert False
    else:
        assert True
