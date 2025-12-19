from __future__ import annotations

from typing import Any, Dict, List

from pymongo import MongoClient
from pyspark.sql import SparkSession

from bigdataspark.env import env
from bigdataspark.dwh import read_dwh_tables
from bigdataspark.marts import build_marts


def _write_collection(
    client: MongoClient, *, db_name: str, collection_name: str, id_field: str, rows: List[Dict[str, Any]]
) -> None:
    db = client[db_name]
    col = db[collection_name]
    col.delete_many({})

    for row in rows:
        row["_id"] = row[id_field]

    if rows:
        col.insert_many(rows, ordered=False)


def main() -> None:
    mongo_uri = env("MONGO_URI", "mongodb://mongo:27017") or "mongodb://mongo:27017"
    mongo_db = env("MONGO_DB", "reports") or "reports"

    spark = (
        SparkSession.builder.appName("dwh-to-mongo")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    marts = build_marts(read_dwh_tables(spark))

    client = MongoClient(mongo_uri)
    try:
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_sales_products",
            id_field="product_id",
            rows=[r.asDict(recursive=True) for r in marts["mart_sales_products"].collect()],
        )
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_sales_customers",
            id_field="customer_id",
            rows=[r.asDict(recursive=True) for r in marts["mart_sales_customers"].collect()],
        )
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_sales_time",
            id_field="year_month",
            rows=[r.asDict(recursive=True) for r in marts["mart_sales_time"].collect()],
        )
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_sales_stores",
            id_field="store_id",
            rows=[r.asDict(recursive=True) for r in marts["mart_sales_stores"].collect()],
        )
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_sales_suppliers",
            id_field="supplier_id",
            rows=[r.asDict(recursive=True) for r in marts["mart_sales_suppliers"].collect()],
        )
        _write_collection(
            client,
            db_name=mongo_db,
            collection_name="mart_product_quality",
            id_field="product_id",
            rows=[r.asDict(recursive=True) for r in marts["mart_product_quality"].collect()],
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()

