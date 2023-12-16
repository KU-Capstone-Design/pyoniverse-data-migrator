import os

from mylib.migrator.document_to_relation import DocumentToRelationMigrator


def test_convert_format(maria_driver, mongo_driver):
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
