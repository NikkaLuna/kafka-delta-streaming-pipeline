# Databricks notebook source


"""
02_bronze_to_silver_cleanse.py

This script performs a batch transformation of raw Kafka-derived events
stored in the Bronze Delta table into a curated Silver table.

Steps:
- Read from Bronze table
- Filter invalid records
- Deduplicate by event_id + timestamp
- Write to partitioned Silver table (Delta)
- ZORDER optimization & AQE recommendations included

Used in: Kafka → Delta → MLflow pipeline (portfolio project)
"""


# Step 1: Read from Bronze Delta table (batch mode)
df_bronze = spark.read.table("bronze_events")

print(f"Bronze record count: {df_bronze.count()}")



# Step 2: Schema enforcement and cleansing

# Filter out invalid or incomplete records
from pyspark.sql.functions import col

# Silver Table Cleansing Step

df_cleaned = (
    df_bronze
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("event_type").isin("click", "purchase", "view"))
)

# Drop duplicates (based on event_id + timestamp)
df_cleaned = df_cleaned.dropDuplicates(["event_id", "timestamp"])

# Optional: Select only relevant cleaned columns
df_cleaned = df_cleaned.select("event_id", "event_type", "timestamp")

# Show top 10 cleaned records
df_cleaned.show(10, truncate=False)  # Or use display(df_cleaned.limit(10)) in notebook

print(f"Cleaned record count: {df_cleaned.count()}")



# Step 3: Deduplicate by event_id, keeping most recent timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("event_id").orderBy(col("timestamp").desc())

df_deduped = (
    df_cleaned
    .withColumn("rn", row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

print(f"Deduplicated record count: {df_deduped.count()}")



# Step 4: Write to Silver Delta table (partitioned by event_type)
# Write cleaned, deduplicated records to Silver Delta table (partitioned by event_type)
(
    df_deduped.write
    .format("delta")
    .mode("overwrite")  # Overwrite for dev; use append for production
    .partitionBy("event_type")
    .option("mergeSchema", "true")
    .saveAsTable("silver_events")
)



# Step 5: Optimization guidance 
# Post-Write Optimization (run manually in %sql cells inside Databricks)

# %sql
# OPTIMIZE silver_events ZORDER BY (timestamp);

# %sql
# ALTER TABLE silver_events SET TBLPROPERTIES (
#   'delta.autoOptimize.optimizeWrite' = true,
#   'delta.autoOptimize.autoCompact' = true
# );



# Step 6: Enable Adaptive Query Execution (typically cluster-level config)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")



# Step 7: Sample validation query (run in SQL cell)
# %sql
# SELECT *
# FROM silver_events
# WHERE event_type = 'click'
#   AND timestamp >= '2025-04-01'
# ORDER BY timestamp DESC
# LIMIT 50;




