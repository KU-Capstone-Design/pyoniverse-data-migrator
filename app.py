import logging
import os
from argparse import ArgumentParser

import dotenv
import pymysql
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymysql.cursors import DictCursor

from mylib.driver.maria import MariaDriver
from mylib.driver.mongo import MongoDriver
from mylib.migrator.document_to_relation import DocumentToRelationMigrator
from mylib.migrator.relation_to_document import RelationToDocumentMigrator


dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)

parser = ArgumentParser()
parser.add_argument("--src", required=True, help="Source Database: mongo|maria")
parser.add_argument("--dest", required=True, help="Destination Database: maria|mongo")

maria_client = pymysql.connect(
    host=os.getenv("MARIA_HOST"),
    port=int(os.getenv("MARIA_PORT")),
    user=os.getenv("MARIA_USER"),
    password=os.getenv("MARIA_PASSWORD"),
    database=os.getenv("MARIA_DB"),
    cursorclass=DictCursor,
)
mongo_client = MongoClient(os.getenv("MONGO_URI"))
try:
    mongo_client.admin.command("ping")
except ConnectionFailure:
    raise RuntimeError("Mongo Server not available")

maria_driver = MariaDriver(client=maria_client)
mongo_driver = MongoDriver(client=mongo_client)

if __name__ == "__main__":
    args = parser.parse_args()
    # select migrator
    match (args.src, args.dest):
        case ("mongo", "maria"):
            migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
        case ("maria", "mongo"):
            migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
        case _:
            raise RuntimeError(f"src: {args.src}, dest: {args.dest} 는 허용되지 않는 조합입니다.")

    logging.info(f"Migrate from {args.src} to {args.dest}: {os.getenv('MONGO_DB'), os.getenv('MARIA_DB')}")
    migrator.migrate("products")
    logging.info("Success!!")
