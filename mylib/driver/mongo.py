import json
import logging
import os
from dataclasses import asdict
from typing import Iterable

import boto3
from pymongo import MongoClient, ReadPreference

from mylib.driver.sqs.model import Data, Message
from mylib.interface import Driver


class MongoDriver(Driver):
    def __init__(self, client: MongoClient):
        self.__client = client

    def read(self, rel: str, n: int = 0) -> Iterable[dict]:
        db = self.__client.get_database(os.getenv("MONGO_DB"), read_preference=ReadPreference.SECONDARY_PREFERRED)
        coll = db.get_collection(rel)
        data = coll.find(projection={"_id": False, "created_at": False, "updated_at": False}).sort("id", 1).limit(n)
        return data

    def write(self, rel: str, updated: Iterable[dict]) -> None:
        """
        SQS로 데이터를 보낸다.
        """
        updated = list(updated)
        for idx in range(0, len(updated), 100):
            message = Message(
                db_name=os.getenv("MONGO_DB"),
                rel_name=rel,
                origin="migrator",
                action="UPDATE",
                filters=[],
                data=[Data(column="documents", value=updated[idx : idx + 100])],
            )
            client = boto3.client("sqs")
            queue_url = client.get_queue_url(QueueName=os.getenv("DB_QUEUE_NAME"))["QueueUrl"]
            client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(asdict(message)),
            )
            logging.info(f"Send {idx + 100}/{len(updated)}")
