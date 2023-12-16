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
            case "product_bests":
                for s in src:
                    yield {"product_id": int(s["id"]), "brand": int(s["best"]["brand"]), "price": s["best"]["price"]}
            case "product_bests_product_events":
                for s in src:
                    for e in s["best"]["events"]:
                        yield {"product_id": int(s["id"]), "event_id": int(e)}
            case "product_brands":
                for s in src:
                    for b in s["brands"]:
                        yield {
                            "product_id": int(s["id"]),
                            "brand_id": int(b["id"]),
                            "price": int(b["price"]["value"]),
                            "event_price": int(b["price"]["discounted_value"]),
                        }
            case "product_brands_product_events":
                for s in src:
                    for b in s["brands"]:
                        for e in b["events"]:
                            yield {"product_id": int(s["id"]), "brand_id": int(b["id"]), "event_id": int(e)}
            case _:
                raise RuntimeError(f"{rel}은 지원하지 않습니다.")
