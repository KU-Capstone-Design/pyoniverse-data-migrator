from itertools import tee

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
