from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import SparkSession

from bigdataspark.env import env
from bigdataspark.dwh import read_dwh_tables
from bigdataspark.marts import build_marts


def _chunks(items: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _wait_for_cassandra(host: str, port: int, *, timeout_s: int = 180) -> Tuple[Cluster, Session]:
    deadline = time.time() + timeout_s
    last_exc: Optional[Exception] = None

    while time.time() < deadline:
        try:
            cluster = Cluster([host], port=port)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(3)

    raise RuntimeError(f"Cannot connect to Cassandra at {host}:{port}") from last_exc


def _create_schema(session: Session, keyspace: str) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_sales_products (
          product_id int PRIMARY KEY,
          product_name text,
          product_category text,
          product_brand text,
          total_sales_quantity double,
          total_revenue double,
          avg_rating double,
          product_reviews int,
          sales_rows bigint,
          sales_rank int,
          category_revenue double
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_sales_customers (
          customer_id int PRIMARY KEY,
          customer_first_name text,
          customer_last_name text,
          customer_email text,
          customer_country text,
          total_revenue double,
          total_items double,
          sales_rows bigint,
          avg_check double,
          revenue_rank int
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_sales_time (
          year int,
          month int,
          year_month text,
          total_revenue double,
          total_items double,
          sales_rows bigint,
          avg_order_value double,
          PRIMARY KEY ((year), month)
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_sales_stores (
          store_id int PRIMARY KEY,
          store_name text,
          store_city text,
          store_state text,
          store_country text,
          store_email text,
          total_revenue double,
          total_items double,
          sales_rows bigint,
          avg_check double,
          revenue_rank int
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_sales_suppliers (
          supplier_id int PRIMARY KEY,
          supplier_name text,
          supplier_email text,
          supplier_city text,
          supplier_country text,
          total_revenue double,
          total_items double,
          sales_rows bigint,
          avg_product_price double,
          revenue_rank int
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_product_quality (
          product_id int PRIMARY KEY,
          product_name text,
          total_sales_quantity double,
          total_revenue double,
          avg_rating double,
          product_reviews int,
          corr_rating_sales_qty double,
          rating_rank_desc int,
          rating_rank_asc int,
          reviews_rank_desc int
        );
        """
    )


def _truncate_all(session: Session) -> None:
    for table in (
        "mart_sales_products",
        "mart_sales_customers",
        "mart_sales_time",
        "mart_sales_stores",
        "mart_sales_suppliers",
        "mart_product_quality",
    ):
        session.execute(f"TRUNCATE {table};")


def _insert_many(
    session: Session, *, table: str, columns: List[str], rows: List[Dict[str, Any]], concurrency: int = 64
) -> None:
    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(columns)
    prepared = session.prepare(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders});")

    args = []
    for row in rows:
        args.append([row.get(col) for col in columns])

    for results in execute_concurrent_with_args(session, prepared, args, concurrency=concurrency):
        ok, res = results
        if not ok:
            raise RuntimeError(f"Failed inserting into {table}: {res}") from res


def main() -> None:
    cassandra_host = env("CASSANDRA_HOST", "cassandra") or "cassandra"
    cassandra_port = int(env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = env("CASSANDRA_KEYSPACE", "reports") or "reports"

    spark = (
        SparkSession.builder.appName("dwh-to-cassandra")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    marts = build_marts(read_dwh_tables(spark))

    cluster, session = _wait_for_cassandra(cassandra_host, cassandra_port)
    try:
        _create_schema(session, keyspace)
        _truncate_all(session)

        products_rows = [r.asDict(recursive=True) for r in marts["mart_sales_products"].collect()]
        customers_rows = [r.asDict(recursive=True) for r in marts["mart_sales_customers"].collect()]
        time_rows = [r.asDict(recursive=True) for r in marts["mart_sales_time"].collect()]
        stores_rows = [r.asDict(recursive=True) for r in marts["mart_sales_stores"].collect()]
        suppliers_rows = [r.asDict(recursive=True) for r in marts["mart_sales_suppliers"].collect()]
        quality_rows = [r.asDict(recursive=True) for r in marts["mart_product_quality"].collect()]

        _insert_many(
            session,
            table="mart_sales_products",
            columns=[
                "product_id",
                "product_name",
                "product_category",
                "product_brand",
                "total_sales_quantity",
                "total_revenue",
                "avg_rating",
                "product_reviews",
                "sales_rows",
                "sales_rank",
                "category_revenue",
            ],
            rows=products_rows,
        )
        _insert_many(
            session,
            table="mart_sales_customers",
            columns=[
                "customer_id",
                "customer_first_name",
                "customer_last_name",
                "customer_email",
                "customer_country",
                "total_revenue",
                "total_items",
                "sales_rows",
                "avg_check",
                "revenue_rank",
            ],
            rows=customers_rows,
        )
        _insert_many(
            session,
            table="mart_sales_time",
            columns=[
                "year",
                "month",
                "year_month",
                "total_revenue",
                "total_items",
                "sales_rows",
                "avg_order_value",
            ],
            rows=time_rows,
        )
        _insert_many(
            session,
            table="mart_sales_stores",
            columns=[
                "store_id",
                "store_name",
                "store_city",
                "store_state",
                "store_country",
                "store_email",
                "total_revenue",
                "total_items",
                "sales_rows",
                "avg_check",
                "revenue_rank",
            ],
            rows=stores_rows,
        )
        _insert_many(
            session,
            table="mart_sales_suppliers",
            columns=[
                "supplier_id",
                "supplier_name",
                "supplier_email",
                "supplier_city",
                "supplier_country",
                "total_revenue",
                "total_items",
                "sales_rows",
                "avg_product_price",
                "revenue_rank",
            ],
            rows=suppliers_rows,
        )
        _insert_many(
            session,
            table="mart_product_quality",
            columns=[
                "product_id",
                "product_name",
                "total_sales_quantity",
                "total_revenue",
                "avg_rating",
                "product_reviews",
                "corr_rating_sales_qty",
                "rating_rank_desc",
                "rating_rank_asc",
                "reviews_rank_desc",
            ],
            rows=quality_rows,
        )
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
