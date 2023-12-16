import os

from mylib.migrator.document_to_relation import DocumentToRelationMigrator


def test_convert_format_product(maria_driver, mongo_driver):
    migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
    src_data = mongo_driver.read(os.getenv("MONGO_DB"), "products", 100)
    dest_data = migrator._convert("products", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["id"])):
        assert d["category_id"] == int(s["category"] or 0)
        assert d["name"] == s["name"]
        assert d["description"] == s["description"]
        assert d["price"] == s["price"]
        assert d["image"] == s["image"]
        assert d["good_count"] == int(s["good_count"])
        assert d["view_count"] == int(s["view_count"])


def test_convert_format_product_bests(maria_driver, mongo_driver):
    migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
    src_data = mongo_driver.read(os.getenv("MONGO_DB"), "products", 100)
    dest_data = migrator._convert("product_bests", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["product_id"])):
        assert d["brand_id"] == int(s["best"]["brand"])
        assert d["price"] == s["best"]["price"]


def test_convert_format_product_bests_product_events(maria_driver, mongo_driver):
    migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
    src_data = mongo_driver.read(os.getenv("MONGO_DB"), "products", 100)
    dest_data = migrator._convert("product_bests_product_events", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["product_id"])):
        for e in s["best"]["events"]:
            assert d["event_id"] == int(e)


def test_convert_format_product_brands(maria_driver, mongo_driver):
    migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
    src_data = mongo_driver.read(os.getenv("MONGO_DB"), "products", 100)
    dest_data = migrator._convert("product_brands", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["product_id"])):
        for b in s["brands"]:
            assert d["brand_id"] == int(b["id"])
            assert d["price"] == b["price"]["value"]
            assert d["event_price"] == b["price"]["discounted_value"]


def test_convert_format_product_brands_product_events(maria_driver, mongo_driver):
    migrator = DocumentToRelationMigrator(src_driver=mongo_driver, dest_driver=maria_driver)
    src_data = mongo_driver.read(os.getenv("MONGO_DB"), "products", 100)
    dest_data = migrator._convert("product_brands_product_events", src_data)
    for s, d in zip(sorted(src_data, key=lambda x: x["id"]), sorted(dest_data, key=lambda x: x["product_id"])):
        for b in s["brands"]:
            assert d["brand_id"] == int(b["id"])
            for e in b["events"]:
                assert d["event_id"] == int(e)
