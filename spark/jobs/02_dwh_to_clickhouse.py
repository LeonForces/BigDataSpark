from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from bigdataspark.env import env
from bigdataspark.jdbc import ch_props, ch_url
from bigdataspark.dwh import read_dwh_tables
from bigdataspark.marts import build_marts


def _write_clickhouse(df, table: str, *, order_by: str) -> None:
    props = ch_props()
    create_opts = f"ENGINE = MergeTree() ORDER BY ({order_by})"

    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    non_string_cols = [field.name for field in df.schema.fields if field.name not in set(string_cols)]
    df = df.fillna("", subset=string_cols).fillna(0, subset=non_string_cols)

    (
        df.write.format("jdbc")
        .option("url", ch_url())
        .option("driver", props["driver"])
        .option("dbtable", table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("createTableOptions", create_opts)
        .mode("overwrite")
        .save()
    )


def main() -> None:
    dwh_schema = env("PG_DWH_SCHEMA", "dwh") or "dwh"

    spark = (
        SparkSession.builder.appName("dwh-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    marts = build_marts(read_dwh_tables(spark, dwh_schema=dwh_schema))

    _write_clickhouse(marts["mart_sales_products"], "mart_sales_products", order_by="product_id")
    _write_clickhouse(marts["mart_sales_customers"], "mart_sales_customers", order_by="customer_id")
    _write_clickhouse(marts["mart_sales_time"], "mart_sales_time", order_by="year, month")
    _write_clickhouse(marts["mart_sales_stores"], "mart_sales_stores", order_by="store_id")
    _write_clickhouse(marts["mart_sales_suppliers"], "mart_sales_suppliers", order_by="supplier_id")
    _write_clickhouse(marts["mart_product_quality"], "mart_product_quality", order_by="product_id")


if __name__ == "__main__":
    main()
