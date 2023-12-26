import logging
from typing import Iterable, List

import pymysql

from mylib.interface import Driver


class MariaDriver(Driver):
    def __init__(self, client: pymysql.Connect):
        self.__client = client

    def read(self, rel: str, n: int = None) -> Iterable[dict]:
        with self.__client.cursor() as cursor:
            if n is not None:
                query = f"""
                SELECT * FROM {rel}
                LIMIT {n};
                """
            else:
                query = f"""
                SELECT * FROM {rel};
                """
            cursor.execute(query)
            for _ in range(cursor.rowcount):
                yield cursor.fetchone()

    def write(self, rel: str, updated: Iterable[dict]) -> None:
        queries: List[str] = [self.__make_query(rel, d) for d in updated]
        with self.__client.cursor() as cursor:
            cursor.execute("SET SESSION autocommit=0;")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute("begin;")
            for query in queries:
                try:
                    cursor.execute(query)
                except Exception as e:
                    cursor.execute("rollback;")
                    raise RuntimeError(f"쿼리 실행에 문제가 생겼습니다: {query}")
            cursor.execute("commit;")

    def __make_query(self, rel: str, datum: dict) -> str:
        datum = {k: v for k, v in datum.items() if v is not None}
        attrs = ",".join(datum.keys())
        vals = []
        for val in datum.values():
            if isinstance(val, str):
                vals.append(f'"{val}"')
            else:
                vals.append(str(val))
        vals = ",".join(vals)
        updates = []
        for key, val in datum.items():
            if key == "id":
                continue
            if isinstance(val, str):
                updates.append(f'{key}="{val}"')
            else:
                updates.append(f"{key}={val}")
        updates = ",".join(updates)

        query = f"INSERT INTO {rel}({attrs}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {updates};"
        return query
