from __future__ import annotations

from typing import Any, Dict, Iterable, List

from neo4j import GraphDatabase
from pyspark.sql import SparkSession

from bigdataspark.env import env
from bigdataspark.dwh import read_dwh_tables
from bigdataspark.marts import build_marts


def _chunks(items: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _ensure_constraints(session) -> None:
    session.run(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ProductMart) REQUIRE n.product_id IS UNIQUE;"
    )
    session.run(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:CustomerMart) REQUIRE n.customer_id IS UNIQUE;"
    )
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:TimeMart) REQUIRE n.year_month IS UNIQUE;")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:StoreMart) REQUIRE n.store_id IS UNIQUE;")
    session.run(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SupplierMart) REQUIRE n.supplier_id IS UNIQUE;"
    )
    session.run(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ProductQualityMart) REQUIRE n.product_id IS UNIQUE;"
    )


def _upsert_nodes(session, *, label: str, id_field: str, rows: List[Dict[str, Any]]) -> None:
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{{id_field}: row.{id_field}}})
    SET n += row
    """
    for chunk in _chunks(rows, 1000):
        session.run(query, rows=chunk)


def main() -> None:
    neo4j_uri = env("NEO4J_URI", "neo4j://neo4j:7687") or "neo4j://neo4j:7687"
    neo4j_user = env("NEO4J_USER", "neo4j") or "neo4j"
    neo4j_password = env("NEO4J_PASSWORD", "password") or "password"

    spark = (
        SparkSession.builder.appName("dwh-to-neo4j")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    marts = build_marts(read_dwh_tables(spark))

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as session:
            _ensure_constraints(session)

            _upsert_nodes(
                session,
                label="ProductMart",
                id_field="product_id",
                rows=[r.asDict(recursive=True) for r in marts["mart_sales_products"].collect()],
            )
            _upsert_nodes(
                session,
                label="CustomerMart",
                id_field="customer_id",
                rows=[r.asDict(recursive=True) for r in marts["mart_sales_customers"].collect()],
            )
            _upsert_nodes(
                session,
                label="TimeMart",
                id_field="year_month",
                rows=[r.asDict(recursive=True) for r in marts["mart_sales_time"].collect()],
            )
            _upsert_nodes(
                session,
                label="StoreMart",
                id_field="store_id",
                rows=[r.asDict(recursive=True) for r in marts["mart_sales_stores"].collect()],
            )
            _upsert_nodes(
                session,
                label="SupplierMart",
                id_field="supplier_id",
                rows=[r.asDict(recursive=True) for r in marts["mart_sales_suppliers"].collect()],
            )
            _upsert_nodes(
                session,
                label="ProductQualityMart",
                id_field="product_id",
                rows=[r.asDict(recursive=True) for r in marts["mart_product_quality"].collect()],
            )
    finally:
        driver.close()


if __name__ == "__main__":
    main()

