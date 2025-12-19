from __future__ import annotations

from pyspark.sql import SparkSession, Window, functions as F

from bigdataspark.env import env
from bigdataspark.jdbc import pg_props, pg_url


def _trim(col_name: str) -> F.Column:
    return F.trim(F.col(col_name))


def main() -> None:
    raw_table = env("PG_RAW_TABLE", "mock_data") or "mock_data"
    dwh_schema = env("PG_DWH_SCHEMA", "dwh") or "dwh"

    spark = (
        SparkSession.builder.appName("raw-to-dwh")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    raw_df = spark.read.jdbc(pg_url(), raw_table, properties=pg_props())

    typed_df = (
        raw_df.select(
            _trim("id").cast("int").alias("sale_id"),
            _trim("sale_customer_id").cast("int").alias("customer_id"),
            _trim("sale_seller_id").cast("int").alias("seller_id"),
            _trim("sale_product_id").cast("int").alias("product_id"),
            F.to_date(_trim("sale_date"), "M/d/yyyy").alias("sale_date"),
            _trim("sale_quantity").cast("int").alias("sale_quantity"),
            _trim("sale_total_price").cast("decimal(12,2)").alias("sale_total_price"),
            _trim("product_price").cast("decimal(12,2)").alias("product_price"),
            _trim("product_quantity").cast("int").alias("product_quantity"),
            _trim("customer_first_name").alias("customer_first_name"),
            _trim("customer_last_name").alias("customer_last_name"),
            _trim("customer_age").cast("int").alias("customer_age"),
            _trim("customer_email").alias("customer_email"),
            _trim("customer_country").alias("customer_country"),
            _trim("customer_postal_code").alias("customer_postal_code"),
            _trim("customer_pet_type").alias("customer_pet_type"),
            _trim("customer_pet_name").alias("customer_pet_name"),
            _trim("customer_pet_breed").alias("customer_pet_breed"),
            _trim("pet_category").alias("pet_category"),
            _trim("seller_first_name").alias("seller_first_name"),
            _trim("seller_last_name").alias("seller_last_name"),
            _trim("seller_email").alias("seller_email"),
            _trim("seller_country").alias("seller_country"),
            _trim("seller_postal_code").alias("seller_postal_code"),
            _trim("product_name").alias("product_name"),
            _trim("product_category").alias("product_category"),
            _trim("product_weight").cast("double").alias("product_weight"),
            _trim("product_color").alias("product_color"),
            _trim("product_size").alias("product_size"),
            _trim("product_brand").alias("product_brand"),
            _trim("product_material").alias("product_material"),
            F.col("product_description").alias("product_description"),
            _trim("product_rating").cast("double").alias("product_rating"),
            _trim("product_reviews").cast("int").alias("product_reviews"),
            F.to_date(_trim("product_release_date"), "M/d/yyyy").alias("product_release_date"),
            F.to_date(_trim("product_expiry_date"), "M/d/yyyy").alias("product_expiry_date"),
            _trim("store_name").alias("store_name"),
            _trim("store_location").alias("store_location"),
            _trim("store_city").alias("store_city"),
            _trim("store_state").alias("store_state"),
            _trim("store_country").alias("store_country"),
            _trim("store_phone").alias("store_phone"),
            _trim("store_email").alias("store_email"),
            _trim("supplier_name").alias("supplier_name"),
            _trim("supplier_contact").alias("supplier_contact"),
            _trim("supplier_email").alias("supplier_email"),
            _trim("supplier_phone").alias("supplier_phone"),
            _trim("supplier_address").alias("supplier_address"),
            _trim("supplier_city").alias("supplier_city"),
            _trim("supplier_country").alias("supplier_country"),
        )
        .filter(F.col("sale_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("seller_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("sale_date").isNotNull())
    )

    dim_customer = typed_df.select(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "pet_category",
    ).dropDuplicates(["customer_id"])

    dim_seller = typed_df.select(
        "seller_id",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ).dropDuplicates(["seller_id"])

    dim_product = typed_df.select(
        "product_id",
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
    ).dropDuplicates(["product_id"])

    store_nk = F.concat_ws(
        "||",
        F.coalesce(F.col("store_email"), F.lit("")),
        F.coalesce(F.col("store_name"), F.lit("")),
        F.coalesce(F.col("store_location"), F.lit("")),
        F.coalesce(F.col("store_city"), F.lit("")),
        F.coalesce(F.col("store_state"), F.lit("")),
        F.coalesce(F.col("store_country"), F.lit("")),
        F.coalesce(F.col("store_phone"), F.lit("")),
    )
    dim_store_base = (
        typed_df.select(
            store_nk.alias("store_nk"),
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_nk"])
        .filter(F.col("store_nk").isNotNull())
    )
    dim_store = dim_store_base.withColumn(
        "store_id", F.dense_rank().over(Window.orderBy(F.col("store_nk")))
    ).select(
        "store_id",
        "store_nk",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    )

    supplier_nk = F.concat_ws(
        "||",
        F.coalesce(F.col("supplier_email"), F.lit("")),
        F.coalesce(F.col("supplier_name"), F.lit("")),
        F.coalesce(F.col("supplier_contact"), F.lit("")),
        F.coalesce(F.col("supplier_phone"), F.lit("")),
        F.coalesce(F.col("supplier_address"), F.lit("")),
        F.coalesce(F.col("supplier_city"), F.lit("")),
        F.coalesce(F.col("supplier_country"), F.lit("")),
    )
    dim_supplier_base = (
        typed_df.select(
            supplier_nk.alias("supplier_nk"),
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_nk"])
        .filter(F.col("supplier_nk").isNotNull())
    )
    dim_supplier = dim_supplier_base.withColumn(
        "supplier_id", F.dense_rank().over(Window.orderBy(F.col("supplier_nk")))
    ).select(
        "supplier_id",
        "supplier_nk",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    )

    dim_date = (
        typed_df.select(F.col("sale_date").alias("date"))
        .dropDuplicates(["date"])
        .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date").cast("int"))
        .withColumn("month", F.month("date").cast("int"))
        .withColumn("day", F.dayofmonth("date").cast("int"))
        .withColumn("quarter", F.quarter("date").cast("int"))
        .select("date_id", "date", "year", "month", "day", "quarter")
    )

    fact_base = typed_df.select(
        "sale_id",
        "sale_date",
        "customer_id",
        "seller_id",
        "product_id",
        "sale_quantity",
        "sale_total_price",
        store_nk.alias("store_nk"),
        supplier_nk.alias("supplier_nk"),
    )
    fact_sales = (
        fact_base.join(dim_store.select("store_id", "store_nk"), on="store_nk", how="left")
        .join(dim_supplier.select("supplier_id", "supplier_nk"), on="supplier_nk", how="left")
        .withColumn("date_id", F.date_format("sale_date", "yyyyMMdd").cast("int"))
        .select(
            "sale_id",
            "date_id",
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            "supplier_id",
            "sale_quantity",
            "sale_total_price",
        )
    )

    url = pg_url()
    props = pg_props()

    dim_customer.write.jdbc(url, f"{dwh_schema}.dim_customer", mode="overwrite", properties=props)
    dim_seller.write.jdbc(url, f"{dwh_schema}.dim_seller", mode="overwrite", properties=props)
    dim_product.write.jdbc(url, f"{dwh_schema}.dim_product", mode="overwrite", properties=props)
    dim_store.write.jdbc(url, f"{dwh_schema}.dim_store", mode="overwrite", properties=props)
    dim_supplier.write.jdbc(url, f"{dwh_schema}.dim_supplier", mode="overwrite", properties=props)
    dim_date.write.jdbc(url, f"{dwh_schema}.dim_date", mode="overwrite", properties=props)
    fact_sales.write.jdbc(url, f"{dwh_schema}.fact_sales", mode="overwrite", properties=props)


if __name__ == "__main__":
    main()
