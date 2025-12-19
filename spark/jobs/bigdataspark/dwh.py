from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from .env import env
from .jdbc import pg_props, pg_url


def read_dwh_tables(spark: SparkSession, *, dwh_schema: Optional[str] = None) -> Dict[str, DataFrame]:
    schema = dwh_schema or (env("PG_DWH_SCHEMA", "dwh") or "dwh")
    url = pg_url()
    props = pg_props()

    def read(table: str) -> DataFrame:
        return spark.read.jdbc(url, f"{schema}.{table}", properties=props)

    return {
        "fact_sales": read("fact_sales"),
        "dim_product": read("dim_product"),
        "dim_customer": read("dim_customer"),
        "dim_store": read("dim_store"),
        "dim_supplier": read("dim_supplier"),
        "dim_date": read("dim_date"),
    }
