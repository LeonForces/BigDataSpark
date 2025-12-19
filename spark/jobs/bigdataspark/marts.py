from typing import Dict

from pyspark.sql import DataFrame, Window, functions as F


def build_marts(dwh: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    fact = dwh["fact_sales"]
    dim_product = dwh["dim_product"]
    dim_customer = dwh["dim_customer"]
    dim_store = dwh["dim_store"]
    dim_supplier = dwh["dim_supplier"]
    dim_date = dwh["dim_date"]

    products_mart = (
        fact.join(
            dim_product.select(
                "product_id",
                "product_name",
                "product_category",
                "product_brand",
                "product_rating",
                "product_reviews",
            ),
            on="product_id",
            how="left",
        )
        .groupBy("product_id", "product_name", "product_category", "product_brand")
        .agg(
            F.sum("sale_quantity").cast("double").alias("total_sales_quantity"),
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.avg("product_rating").alias("avg_rating"),
            F.max("product_reviews").alias("product_reviews"),
            F.count("*").alias("sales_rows"),
        )
        .withColumn(
            "sales_rank",
            F.dense_rank().over(
                Window.orderBy(F.col("total_sales_quantity").desc(), F.col("total_revenue").desc())
            ),
        )
        .withColumn(
            "category_revenue",
            F.sum("total_revenue").over(Window.partitionBy("product_category")),
        )
    )

    customers_mart = (
        fact.join(
            dim_customer.select(
                "customer_id",
                "customer_first_name",
                "customer_last_name",
                "customer_email",
                "customer_country",
            ),
            on="customer_id",
            how="left",
        )
        .groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.sum("sale_quantity").cast("double").alias("total_items"),
            F.count("*").alias("sales_rows"),
            F.avg("sale_total_price").cast("double").alias("avg_check"),
        )
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.col("total_revenue").desc())))
    )

    time_mart = (
        fact.join(dim_date.select("date_id", "year", "month"), on="date_id", how="left")
        .groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.sum("sale_quantity").cast("double").alias("total_items"),
            F.count("*").alias("sales_rows"),
            F.avg("sale_total_price").cast("double").alias("avg_order_value"),
        )
        .withColumn("year_month", F.format_string("%04d-%02d", F.col("year"), F.col("month")))
    )

    stores_mart = (
        fact.join(
            dim_store.select(
                "store_id",
                "store_name",
                "store_city",
                "store_state",
                "store_country",
                "store_email",
            ),
            on="store_id",
            how="left",
        )
        .groupBy("store_id", "store_name", "store_city", "store_state", "store_country", "store_email")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.sum("sale_quantity").cast("double").alias("total_items"),
            F.count("*").alias("sales_rows"),
            F.avg("sale_total_price").cast("double").alias("avg_check"),
        )
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.col("total_revenue").desc())))
    )

    supplier_avg_price = (
        fact.select("supplier_id", "product_id")
        .dropDuplicates(["supplier_id", "product_id"])
        .join(dim_product.select("product_id", "product_price"), on="product_id", how="left")
        .groupBy("supplier_id")
        .agg(F.avg("product_price").cast("double").alias("avg_product_price"))
    )

    suppliers_mart = (
        fact.join(
            dim_supplier.select(
                "supplier_id",
                "supplier_name",
                "supplier_email",
                "supplier_city",
                "supplier_country",
            ),
            on="supplier_id",
            how="left",
        )
        .groupBy("supplier_id", "supplier_name", "supplier_email", "supplier_city", "supplier_country")
        .agg(
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.sum("sale_quantity").cast("double").alias("total_items"),
            F.count("*").alias("sales_rows"),
        )
        .join(supplier_avg_price, on="supplier_id", how="left")
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.col("total_revenue").desc())))
    )

    quality_base = (
        fact.join(
            dim_product.select("product_id", "product_name", "product_rating", "product_reviews"),
            on="product_id",
            how="left",
        )
        .groupBy("product_id", "product_name")
        .agg(
            F.sum("sale_quantity").cast("double").alias("total_sales_quantity"),
            F.sum("sale_total_price").cast("double").alias("total_revenue"),
            F.avg("product_rating").alias("avg_rating"),
            F.max("product_reviews").alias("product_reviews"),
        )
    )

    corr_value = quality_base.select(F.corr("avg_rating", "total_sales_quantity")).collect()[0][0]
    if corr_value is None:
        corr_value = 0.0

    quality_mart = (
        quality_base.withColumn("corr_rating_sales_qty", F.lit(float(corr_value)))
        .withColumn("rating_rank_desc", F.dense_rank().over(Window.orderBy(F.col("avg_rating").desc())))
        .withColumn("rating_rank_asc", F.dense_rank().over(Window.orderBy(F.col("avg_rating").asc())))
        .withColumn(
            "reviews_rank_desc", F.dense_rank().over(Window.orderBy(F.col("product_reviews").desc()))
        )
    )

    return {
        "mart_sales_products": products_mart,
        "mart_sales_customers": customers_mart,
        "mart_sales_time": time_mart,
        "mart_sales_stores": stores_mart,
        "mart_sales_suppliers": suppliers_mart,
        "mart_product_quality": quality_mart,
    }
