# Databricks notebook source
# Databricks notebook source
# Phase 1 - Gold Transformation
# Builds business-level, aggregated tables meant to directly answer
# real analytical questions. Gold = "ready for a dashboard or an agent
# to query directly."
#
# Business decision: "sales"/revenue tables use DELIVERED orders only,
# since that's the standard definition of realized revenue. Order status
# breakdowns (including canceled/other) are still fully available in
# Silver for anyone who needs that view.

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count as spark_count,
    countDistinct, date_format, round as spark_round
)

CATALOG = "workspace"

orders_enriched = spark.table(f"{CATALOG}.silver.orders_enriched")
order_items = spark.table(f"{CATALOG}.silver.order_items")
products = spark.table(f"{CATALOG}.silver.products")
sellers = spark.table(f"{CATALOG}.silver.sellers")
category_translation = spark.table(f"{CATALOG}.silver.category_translation")

# Filter to delivered orders only -- this defines "sales" for every Gold
# table below. (Silver still has every status if that's ever needed.)
delivered_orders = orders_enriched.filter(col("order_status") == "delivered")

# Build one base dataset joining delivered orders -> order_items -> products
# -> sellers, since category/seller performance both need this combination.
base = (
    delivered_orders
    .join(order_items, on="order_id", how="inner")
    .join(products, on="product_id", how="left")
    .join(sellers, on="seller_id", how="left")
    .join(category_translation, on="product_category_name", how="left")
)

# ---------------------------------------------------------------------------
# GOLD TABLE 1: Sales performance by product category
# ---------------------------------------------------------------------------

category_performance = (
    base.groupBy("product_category_name_english")
    .agg(
        spark_round(spark_sum("price"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("num_orders"),
        spark_round(avg("avg_review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(col("total_revenue").desc())
)

category_performance.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.gold.category_performance")

# ---------------------------------------------------------------------------
# GOLD TABLE 2: Seller performance
# ---------------------------------------------------------------------------

seller_performance = (
    base.groupBy("seller_id", "seller_city", "seller_state")
    .agg(
        spark_round(spark_sum("price"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("num_orders"),
        spark_round(avg("avg_review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(col("total_revenue").desc())
)

seller_performance.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.gold.seller_performance")

# ---------------------------------------------------------------------------
# GOLD TABLE 3: Monthly sales trend
# ---------------------------------------------------------------------------

monthly_sales = (
    delivered_orders
    .join(order_items, on="order_id", how="inner")
    .withColumn("order_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        spark_round(spark_sum("price"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("num_orders"),
    )
    .orderBy("order_month")
)

monthly_sales.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.gold.monthly_sales")

print("Gold transformation complete.")
print(f"category_performance: {category_performance.count()} rows")
print(f"seller_performance: {seller_performance.count()} rows")
print(f"monthly_sales: {monthly_sales.count()} rows")