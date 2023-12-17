from typing import Iterable

import pandas as pd
from pandas import DataFrame

from mylib.interface import Driver, Migrator


class RelationToDocumentMigrator(Migrator):
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
