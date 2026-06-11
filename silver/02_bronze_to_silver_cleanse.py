# Databricks notebook source
# COMMAND ----------

from datetime import datetime
import time

from pyspark.sql import Row
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# Enable Adaptive Query Execution + tuning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.shuffle.partitions", "64")

# COMMAND ----------

# Read from Bronze Delta table in batch mode
df_bronze = spark.read.table("bronze_events")
print(f"Bronze record count: {df_bronze.count()}")

# COMMAND ----------

# Filter, cast, and normalize records for Silver
df_cleaned = (
    df_bronze
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("event_type").isin("click", "purchase", "view"))
    .filter(col("value").isNotNull())
    .withColumn("value", col("value").cast("double"))
    .select("event_id", "event_type", "timestamp", "value")
)

df_cleaned.show(10, truncate=False)

# COMMAND ----------

# Deduplicate by event_id and keep the latest timestamp
window_spec = Window.partitionBy("event_id").orderBy(col("timestamp").desc())

df_deduped = (
    df_cleaned
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Deduplicated record count: {df_deduped.count()}")

# COMMAND ----------

# Explain physical plan for performance inspection
df_deduped.explain(mode="formatted")

# COMMAND ----------

# Write curated records to Silver Delta table and log write duration
start = time.time()

spark.sql("DROP TABLE IF EXISTS silver_events")

(
    df_deduped.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_type")
    .option("mergeSchema", "true")
    .saveAsTable("silver_events")
)

end = time.time()
elapsed_seconds = end - start

print(f"Elapsed time: {elapsed_seconds:.2f} seconds")

# COMMAND ----------

# Log write benchmark to Delta
benchmark_df = spark.createDataFrame([
    Row(
        task="silver_write",
        elapsed_seconds=elapsed_seconds,
        run_time=datetime.utcnow()
    )
])

benchmark_df.write.mode("append").format("delta").saveAsTable("benchmark_logs")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM benchmark_logs
# MAGIC ORDER BY run_time DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_events
# MAGIC WHERE event_type = 'click'
# MAGIC   AND timestamp >= '2025-04-01'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_events
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10