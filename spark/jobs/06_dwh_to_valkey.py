from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Tuple

import redis
from pyspark.sql import SparkSession

from bigdataspark.env import env
from bigdataspark.dwh import read_dwh_tables
from bigdataspark.marts import build_marts


def _chunks(items: List[Tuple[str, str]], chunk_size: int) -> Iterable[List[Tuple[str, str]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _write_hash(
    r: redis.Redis, *, key: str, id_field: str, rows: List[Dict[str, Any]], chunk_size: int = 2000
) -> None:
    r.delete(key)
    pairs: List[Tuple[str, str]] = []

    for row in rows:
        row_id = row.get(id_field)
        if row_id is None:
            continue
        pairs.append((str(row_id), json.dumps(row, ensure_ascii=False)))

    for chunk in _chunks(pairs, chunk_size):
        r.hset(key, mapping=dict(chunk))


def main() -> None:
    host = env("VALKEY_HOST", "valkey") or "valkey"
    port = int(env("VALKEY_PORT", "6379") or "6379")

    spark = (
        SparkSession.builder.appName("dwh-to-valkey")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    marts = build_marts(read_dwh_tables(spark))

    r = redis.Redis(host=host, port=port, decode_responses=True)

    _write_hash(
        r,
        key="reports:mart_sales_products",
        id_field="product_id",
        rows=[r.asDict(recursive=True) for r in marts["mart_sales_products"].collect()],
    )
    _write_hash(
        r,
        key="reports:mart_sales_customers",
        id_field="customer_id",
        rows=[r.asDict(recursive=True) for r in marts["mart_sales_customers"].collect()],
    )
    _write_hash(
        r,
        key="reports:mart_sales_time",
        id_field="year_month",
        rows=[r.asDict(recursive=True) for r in marts["mart_sales_time"].collect()],
    )
    _write_hash(
        r,
        key="reports:mart_sales_stores",
        id_field="store_id",
        rows=[r.asDict(recursive=True) for r in marts["mart_sales_stores"].collect()],
    )
    _write_hash(
        r,
        key="reports:mart_sales_suppliers",
        id_field="supplier_id",
        rows=[r.asDict(recursive=True) for r in marts["mart_sales_suppliers"].collect()],
    )
    _write_hash(
        r,
        key="reports:mart_product_quality",
        id_field="product_id",
        rows=[r.asDict(recursive=True) for r in marts["mart_product_quality"].collect()],
    )


if __name__ == "__main__":
    main()

