from itertools import tee
from typing import Iterable

from mylib.interface import Driver, Migrator


class DocumentToRelationMigrator(Migrator):
    def __init__(self, src_driver: Driver, dest_driver: Driver):
        self.__src_driver = src_driver
        self.__dest_driver = dest_driver

    def migrate(self, rel: str) -> None:
        src = self.__src_driver.read(rel)
        match rel:
            case "products":
                srcs = tee(src, 5)
                rel_name = "products"
                products_dest = self._convert(rel_name, srcs[0])
                self.__dest_driver.write(rel_name, products_dest)
                rel_name = "product_bests"
                product_bests_dest = self._convert(rel_name, srcs[1])
                self.__dest_driver.write(rel_name, product_bests_dest)
                rel_name = "product_bests_product_events"
                product_bests_product_events = self._convert(rel_name, srcs[2])
                self.__dest_driver.write(rel_name, product_bests_product_events)
                rel_name = "product_brands"
                product_brands = self._convert(rel_name, srcs[3])
                self.__dest_driver.write(rel_name, product_brands)
                rel_name = "product_brands_product_events"
                product_brands_product_events = self._convert(rel_name, srcs[4])
                self.__dest_driver.write(rel_name, product_brands_product_events)
            case _:
                raise RuntimeError(f"{rel}은 지원하지 않습니다.")

    def _convert(self, rel: str, src: Iterable[dict]) -> Iterable[dict]:
        match rel:
            case "products":
                for s in src:
                    yield {
                        "id": int(s["id"]),
                        "name": s["name"].replace('"', '\\"'),
                        "category_id": int(s["category"] or 0),
                        "description": s["description"].replace('"', '\\"') if s["description"] else None,
                        "price": s["price"],
                        "image": s["image"],
                        "good_count": int(s["good_count"]),
                        "view_count": int(s["view_count"]),
                    }
            case "product_bests":
                for s in src:
                    yield {"product_id": int(s["id"]), "brand_id": int(s["best"]["brand"]), "price": s["best"]["price"]}
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
                            "price": b["price"]["value"],
                            "event_price": b["price"]["discounted_value"],
                        }
            case "product_brands_product_events":
                for s in src:
                    for b in s["brands"]:
                        for e in b["events"]:
                            yield {"product_id": int(s["id"]), "brand_id": int(b["id"]), "event_id": int(e)}
            case _:
                raise RuntimeError(f"{rel}은 지원하지 않습니다.")
