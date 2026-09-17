# Databricks notebook source
# Databricks notebook source
# Phase 1 - Silver Transformation
# Cleans Bronze tables (fix types, remove exact duplicates) and joins them
# into an enriched, analysis-ready "orders_enriched" table.
# Silver = clean and trustworthy, but still row-level detail (no filtering
# of business meaning happens here -- e.g. we keep ALL order statuses).

from pyspark.sql.functions import col, to_timestamp, count as spark_count

CATALOG = "workspace"

# ---------------------------------------------------------------------------
# STEP 1: Clean individual Bronze tables
# ---------------------------------------------------------------------------

def clean_table(table_name, timestamp_cols=None):
    """
    Reads a Bronze table, drops exact duplicate rows, converts given columns
    to proper timestamps, and returns the cleaned DataFrame.
    """
    df = spark.table(f"{CATALOG}.bronze.{table_name}")

    before_count = df.count()
    df = df.dropDuplicates()  # removes only fully-identical rows
    after_count = df.count()

    if timestamp_cols:
        for c in timestamp_cols:
            df = df.withColumn(c, to_timestamp(col(c)))

    df = df.drop("_ingested_at", "_source_file")

    print(f"{table_name}: {before_count} -> {after_count} rows after dedup")
    return df

orders = clean_table(
    "orders",
    timestamp_cols=[
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
)

customers = clean_table("customers")
payments = clean_table("payments")
reviews = clean_table("reviews", timestamp_cols=["review_creation_date", "review_answer_timestamp"])
order_items = clean_table("order_items", timestamp_cols=["shipping_limit_date"])
products = clean_table("products")
sellers = clean_table("sellers")
category_translation = clean_table("category_translation")

# ---------------------------------------------------------------------------
# STEP 2: Aggregate payments and reviews to one row per order
# ---------------------------------------------------------------------------
# Raw payments/reviews can have MULTIPLE rows per order (e.g. split payments,
# multiple review entries). We aggregate them so our final table stays at
# "one row per order" grain -- otherwise joining would multiply row counts
# and silently corrupt downstream analysis.

from pyspark.sql.functions import sum as spark_sum, avg, first

payments_agg = (
    payments.groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("total_payment_value"),
        spark_count("payment_sequential").alias("num_payment_installments_used"),
        first("payment_type").alias("primary_payment_type"),
    )
)

reviews_agg = (
    reviews.groupBy("order_id")
    .agg(
        avg("review_score").alias("avg_review_score"),
        spark_count("review_id").alias("num_reviews"),
    )
)

# ---------------------------------------------------------------------------
# STEP 3: Join everything into one enriched Silver table
# ---------------------------------------------------------------------------

orders_enriched = (
    orders
    .join(customers, on="customer_id", how="left")
    .join(payments_agg, on="order_id", how="left")
    .join(reviews_agg, on="order_id", how="left")
)

# ---------------------------------------------------------------------------
# STEP 4: Write cleaned tables to Silver schema
# ---------------------------------------------------------------------------

orders_enriched.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.silver.orders_enriched")

order_items.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.silver.order_items")

products.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.silver.products")

sellers.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.silver.sellers")

category_translation.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{CATALOG}.silver.category_translation")

print("Silver transformation complete.")
print(f"orders_enriched row count: {orders_enriched.count()}")