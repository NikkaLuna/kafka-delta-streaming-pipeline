# Databricks notebook source
# COMMAND ----------

# Kafka Configuration (Dev Only – Inline Credentials)
kafka_bootstrap = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
kafka_topic = "stream-input"
kafka_api_key = "MYDFB2FDNLVV2QLG"  # Replace with your actual API key
kafka_api_secret = "htVdfSueWUUkvefpM6i2Zu0p7xZ2QatIb+5L2hwQHpIyBY5K2IBb5YqeS0zbzgt1"  # Replace with your actual secret

kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'
}



# COMMAND ----------

# Step 0: Enable Adaptive Query Execution + Tuning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.shuffle.partitions", "64") 

# COMMAND ----------

# Step 1: Read from Bronze Delta table (batch mode)
df_bronze = spark.read.table("bronze_events")
print(f"Bronze record count: {df_bronze.count()}")

# COMMAND ----------

from pyspark.sql.functions import col

# ✅ Step 2: Filter, cast, deduplicate for Silver table
df_cleaned = (
    df_bronze
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("event_type").isin("click", "purchase", "view"))
    .filter(col("value").isNotNull())   # Ensure value is present
    .withColumn("value", col("value").cast("double"))  # Cast to double
    .dropDuplicates(["event_id", "timestamp"])  # Deduplicate
    .select("event_id", "event_type", "timestamp", "value")  # Keep only relevant columns
)

# 👀 Preview the cleaned records
df_cleaned.show(10, truncate=False)



# COMMAND ----------

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

# COMMAND ----------

# Step 4: Explain physical plan (for performance insight)
df_deduped.explain(mode="formatted")

# COMMAND ----------

from datetime import datetime
import time
from pyspark.sql import Row

start = time.time()

df_deduped = df_deduped.withWatermark("timestamp", "5 minutes")

# Drop the broken table and start fresh
spark.sql("DROP TABLE IF EXISTS silver_events")

# Now rerun your write cell
df_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_type") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_events")

end = time.time()
print(f"Elapsed time: {end - start:.2f} seconds")





# COMMAND ----------

# Step 6: Log write benchmark to Delta
benchmark_df = spark.createDataFrame([
    Row(task="silver_write", elapsed_seconds=end - start, run_time=datetime.utcnow())
])
benchmark_df.write.mode("append").format("delta").saveAsTable("benchmark_logs")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM benchmark_logs ORDER BY run_time DESC
# MAGIC

# COMMAND ----------

# Enable AQE features if needed (typically cluster-level config)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_events
# MAGIC WHERE event_type = 'click'
# MAGIC   AND timestamp >= '2025-04-01'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_events ORDER BY timestamp DESC LIMIT 10;
# MAGIC

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping stream: {stream.name}")
    stream.stop()
