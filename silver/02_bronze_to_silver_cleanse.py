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

# Step 0: Enable Adaptive Query Execution + Tuning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.shuffle.partitions", "64")  # Week 5 tuning


# Step 1: Read from Bronze Delta table (batch mode)
df_bronze = spark.read.table("bronze_events")
print(f"Bronze record count: {df_bronze.count()}")


# Step 2: Schema enforcement and cleansing
from pyspark.sql.functions import col

df_cleaned = (
    df_bronze
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("event_type").isin("click", "purchase", "view"))
    .dropDuplicates(["event_id", "timestamp"])
    .select("event_id", "event_type", "timestamp")
)

df_cleaned.show(10, truncate=False)
print(f"Cleaned record count: {df_cleaned.count()}")


# Step 3: Deduplicate by event_id (keep latest timestamp)
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


# Step 4: Explain physical plan (for performance insight)
df_deduped.explain(mode="formatted")


# Step 5: Write to Silver table with benchmarking
from datetime import datetime
import time
from pyspark.sql import Row

start = time.time()

df_deduped = df_deduped.withWatermark("timestamp", "5 minutes")

df_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_type") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_events")

end = time.time()
print(f"Elapsed time: {end - start:.2f} seconds")


# Step 6: Log write benchmark to Delta
benchmark_df = spark.createDataFrame([
    Row(task="silver_write", elapsed_seconds=end - start, run_time=datetime.utcnow())
])
benchmark_df.write.mode("append").format("delta").saveAsTable("benchmark_logs")


# Step 7: (Optional) Run manual post-write optimizations
# %sql
# OPTIMIZE silver_events ZORDER BY (timestamp);
# ALTER TABLE silver_events SET TBLPROPERTIES (
#   'delta.autoOptimize.optimizeWrite' = true,
#   'delta.autoOptimize.autoCompact' = true
# )

# Step 8: Validate Silver Layer (run in SQL)
# %sql
# SELECT * FROM silver_events WHERE event_type = 'click' ORDER BY timestamp DESC LIMIT 50;
