from typing import Iterable

import pymysql

from mylib.interface import Driver


class MariaDriver(Driver):
    def __init__(self, client: pymysql.Connect):
        self.__client = client

    def read(self, db: str, rel: str, n: int) -> Iterable[dict]:
        with self.__client.cursor() as cursor:
            query = f"""
            SELECT * FROM {rel}
            LIMIT {n}
            """
            cursor.execute(query)
            for _ in range(cursor.rowcount):
                yield cursor.fetchone()

    def write(self, db: str, rel: str, updated: Iterable[dict]) -> None:
        pass
