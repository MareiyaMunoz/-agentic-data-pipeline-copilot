# Databricks notebook source
# Databricks notebook source
# Phase 1 - Bronze Ingestion
# Reads raw Olist CSVs from the Unity Catalog Volume and writes them as
# untouched Bronze Delta tables. No cleaning/transformation happens here --
# Bronze = "what we received, as we received it."

VOLUME_PATH = "/Volumes/workspace/default/olist_raw"
CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"

# Map of source CSV filename -> target Bronze table name
# Adjust filenames if your downloaded Olist files are named slightly differently
TABLES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_products_dataset.csv": "products",
    "olist_customers_dataset.csv": "customers",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

for csv_file, table_name in TABLES.items():
    source_path = f"{VOLUME_PATH}/{csv_file}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(source_path)
    )

    # Add basic ingestion metadata -- useful later for your Data Quality Agent
    # (e.g. checking freshness / when a table was last loaded)
    from pyspark.sql.functions import current_timestamp, lit

    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", lit(csv_file))

    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(target_table)

    print(f"Loaded {csv_file} -> {target_table} ({df.count()} rows)")

print("Bronze ingestion complete.")

# COMMAND ----------

display(spark.sql("SHOW SCHEMAS IN workspace"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))