# Databricks notebook source

# COMMAND ----------

# WHY KAFKA?
# Confluent Cloud Kafka provides scalable, secure real-time ingestion,
# and integrates natively with PySpark Structured Streaming.

# Kafka Configuration
# In production, use .env or Databricks Secrets to protect credentials
kafka_bootstrap = "pkc-zgp5j7.us-south1.gcp.confluent.cloud:9092"
kafka_topic = "stream-input"
kafka_api_key = "YOUR_KAFKA_API_KEY"
kafka_api_secret = "YOUR_KAFKA_API_SECRET"

# STREAM CONFIG OPTIONS
# - startingOffsets = "earliest" for full replay during dev
# - checkpointLocation ensures state recovery across restarts
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'
}



# COMMAND ----------

# RAW KAFKA INGESTION
# Pulls raw Kafka records into a Spark DataFrame with binary format
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

#  Verify structure — key/value are still binary
df_raw.printSchema()



# COMMAND ----------

from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col

# Define schema for parsing JSON payloads in Kafka 'value'
event_schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("value", StringType())

# Parse the 'value' column from Kafka (cast → JSON → explode fields)
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Check parsed structure before writing to Delta
df_parsed.printSchema()



# COMMAND ----------

# Stream parsed Kafka JSON into Bronze Delta table
(
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kafka_checkpoint_bronze")  # Use DBFS path in prod
    .table("bronze_events")
)


