from itertools import tee

import pandas as pd
from pandas import DataFrame

from mylib.migrator.relation_to_document import RelationToDocumentMigrator


def test_convert_format_product(maria_driver, mongo_driver):
    migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
    src_data = maria_driver.read("products", 100)
    dest_data = migrator._convert("products", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["id"])):
        assert d["category"] == int(s["category_id"]) or None
        assert d["name"] == s["name"]
        assert d["description"] == s["description"]
        assert d["price"] == s["price"]
        assert d["image"] == s["image"]
        assert d["good_count"] == int(s["good_count"])
        assert d["view_count"] == int(s["view_count"])


def test_convert_format_product_bests(maria_driver, mongo_driver):
    migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
    src_data = maria_driver.read("product_bests", 100)
    dest_data = migrator._convert("product_bests", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["product_id"]), sorted(dest_data, key=lambda x: x["id"])):
        assert d["best"]["brand"] == int(d["brand_id"])
        assert d["price"] == s["best"]["price"]


def test_convert_format_product_bests_product_events(maria_driver, mongo_driver):
    migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
    src_data, src_tmp = tee(maria_driver.read("product_bests_product_events", 100), 2)
    dest_data = migrator._convert("product_bests_product_events", src_data)
    df = DataFrame(src_tmp)
    df = df.groupby("product_id").agg({"event_id": list})
    rows = df.to_dict("index")
    for s, d in zip(sorted(rows.items(), key=lambda x: x[0]), sorted(dest_data, key=lambda x: x["id"])):
        assert s[1]["event_id"] == d["best"]["events"]


def test_convert_format_product_brands(maria_driver, mongo_driver):
    migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
    src_data, src_tmp = tee(maria_driver.read("product_brands", 100), 2)
    src_df = DataFrame(src_tmp)
    src_df = src_df.groupby(["product_id", "brand_id"], as_index=False).agg(lambda x: x.iloc[0])
    src_df["brands"] = src_df.apply(
        lambda x: {
            "id": int(x["brand_id"]),
            "price": {
                "value": x["price"],
                "discounted_value": x["event_price"] if pd.notna(x["event_price"]) else None,
            },
        },
        axis=1,
    )
    src_df = src_df.groupby("product_id").agg({"brands": list})
    expected = src_df.to_dict("index")
    expected = [{"id": key, **val} for key, val in expected.items()]
    dest_data = migrator._convert("product_brands", src_data)
    for s, d in zip(sorted(expected, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["id"])):
        for sb, db in zip(sorted(s["brands"], key=lambda x: x["id"]), sorted(d["brands"], key=lambda x: x["id"])):
            assert sb["price"] == db["price"]


def test_convert_format_product_brands_product_events(maria_driver, mongo_driver):
    migrator = RelationToDocumentMigrator(src_driver=maria_driver, dest_driver=mongo_driver)
    src_data, src_tmp = tee(maria_driver.read("product_brands_product_events", 100), 2)
    src_df = DataFrame(src_tmp)
    src_df = src_df.groupby(["product_id", "brand_id"], as_index=False).agg({"event_id": list})
    src_df["brands"] = src_df.apply(lambda x: {"id": x["brand_id"], "events": x["event_id"]}, axis=1)
    src_df = src_df.groupby("product_id").agg({"brands": list})
    expected = src_df.to_dict("index")
    expected = [{"id": key, **val} for key, val in expected.items()]
    dest_data = migrator._convert("product_brands_product_events", src_data)
    for s, d in zip(sorted(expected, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["id"])):
        for sb, db in zip(sorted(s["brands"], key=lambda x: x["id"]), sorted(d["brands"], key=lambda x: x["id"])):
            assert sorted(sb["events"]) == db["events"]
