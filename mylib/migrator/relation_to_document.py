from typing import Iterable

import pandas as pd
from pandas import DataFrame

from mylib.interface import Driver, Migrator


class RelationToDocumentMigrator(Migrator):
    def __init__(self, src_driver: Driver, dest_driver: Driver):
        self.__src_driver = src_driver
        self.__dest_driver = dest_driver

    def migrate(self, rel: str) -> None:
        match rel:
            case "products":
                # 1. products
                rel_name = "products"
                src = self.__src_driver.read(rel_name)
                product_dest = self._convert(rel_name, src)
                # 2. product_bests
                rel_name = "product_bests"
                src = self.__src_driver.read(rel_name)
                product_best_dest = self._convert(rel_name, src)
                # 3. product_bests_product_events
                rel_name = "product_bests_product_events"
                src = self.__src_driver.read(rel_name)
                product_bests_product_events_dest = self._convert(rel_name, src)
                # 4. product_brands
                rel_name = "product_brands"
                src = self.__src_driver.read(rel_name)
                product_brands_dest = self._convert(rel_name, src)
                # 5. product_brands_product_events
                rel_name = "product_brands_product_events"
                src = self.__src_driver.read(rel_name)
                product_brands_product_events_dest = self._convert(rel_name, src)
                # 조합
                dest = self.__make_products(
                    product_dest,
                    product_best_dest,
                    product_bests_product_events_dest,
                    product_brands_dest,
                    product_brands_product_events_dest,
                )
                self.__dest_driver.write(rel, dest)
            case _:
                raise RuntimeError(f"지원하지 않습니다: {rel}")

    def _convert(self, rel: str, src: Iterable[dict]) -> Iterable[dict]:
        match rel:
            case "products":
                for s in src:
                    yield {
                        "id": s["id"],
                        "category": int(s["category_id"]) or None,
                        "name": s["name"],
                        "description": s["description"],
                        "price": s["price"],
                        "image": s["image"],
                        "good_count": int(s["good_count"]),
                        "view_count": int(s["view_count"]),
                    }
            case "product_bests":
                for s in src:
                    yield {
                        "id": s["product_id"],
                        "best": {
                            "brand": s["brand_id"],
                            "price": s["price"],
                        },
                    }
            case "product_bests_product_events":
                df = DataFrame(src)
                df = df.groupby("product_id").agg({"event_id": list})
                rows = df.to_dict("index")
                for key, val in rows.items():
                    yield {
                        "id": key,
                        "best": {
                            "events": sorted(val["event_id"]),
                        },
                    }
            case "product_brands":
                df = DataFrame(src)
                df = df.groupby(["product_id", "brand_id"], as_index=False).agg(lambda x: x.iloc[0])
                df["brands"] = df.apply(
                    lambda x: {
                        "id": int(x["brand_id"]),
                        "price": {
                            "value": x["price"],
                            "discounted_value": x["event_price"] if pd.notna(x["event_price"]) else None,
                        },
                    },
                    axis=1,
                )
                df = df.groupby("product_id").agg({"brands": list})
                dest = df.to_dict("index")
                for key, val in dest.items():
                    yield {
                        "id": key,
                        **val,
                    }
            case "product_brands_product_events":
                df = DataFrame(src)
                df = df.groupby(["product_id", "brand_id"], as_index=False).agg({"event_id": list})
                df["brands"] = df.apply(lambda x: {"id": x["brand_id"], "events": x["event_id"]}, axis=1)
                df = df.groupby("product_id").agg({"brands": list})
                dest = df.to_dict("index")
                for key, val in dest.items():
                    yield {
                        "id": key,
                        **val,
                    }

            case _:
                raise RuntimeError(f"{rel}은 지원하지 않습니다.")

    def __make_products(
        self,
        product_dest: Iterable[dict],
        product_best_dest: Iterable[dict],
        product_bests_product_events_dest: Iterable[dict],
        product_brands_dest: Iterable[dict],
        product_brands_product_events_dest: Iterable[dict],
    ) -> Iterable[dict]:
        product_best_map = {x["id"]: x["best"] for x in product_best_dest}
        product_best_events_map = {x["id"]: x["best"] for x in product_bests_product_events_dest}
        product_brands_map = {x["id"]: x["brands"] for x in product_brands_dest}
        product_brands_product_events_map = {x["id"]: x["brands"] for x in product_brands_product_events_dest}
        for product in product_dest:
            id_ = product["id"]
            best = product_best_map[id_]
            if best_event := product_best_events_map.get(id_):
                best_event = best_event["events"]
            else:
                best_event = []
            brands = product_brands_map[id_]
            brands_events = product_brands_product_events_map.get(id_, [])
            # brands 에 event 넣기
            brands_events_map = {x["id"]: x["events"] for x in brands_events}
            for brand in brands:
                brand["events"] = brands_events_map.get(brand["id"], [])
            res = {
                **product,
                "best": {
                    **best,
                    "events": best_event,
                },
                "brands": brands,
            }
            yield res
