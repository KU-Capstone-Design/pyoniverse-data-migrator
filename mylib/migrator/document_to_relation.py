from typing import Iterable

from mylib.interface import Driver, Migrator


class DocumentToRelationMigrator(Migrator):
    def __init__(self, src_driver: Driver, dest_driver: Driver):
        self.__src_driver = src_driver
        self.__dest_driver = dest_driver

    def migrate(self, rel: str) -> None:
        pass

    def _convert(self, rel: str, src: Iterable[dict]) -> Iterable[dict]:
        match rel:
            case "products":
                for s in src:
                    yield {
                        "id": int(s["id"]),
                        "name": s["name"],
                        "category_id": int(s["category"]),
                        "description": s["description"],
                        "price": s["price"],
                        "image": s["image"],
                        "good_count": int(s["good_count"]),
                        "view_count": int(s["view_count"]),
                    }
            case _:
                raise RuntimeError(f"{rel}은 지원하지 않습니다.")
