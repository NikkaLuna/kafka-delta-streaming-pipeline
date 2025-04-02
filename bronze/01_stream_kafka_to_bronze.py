# Databricks notebook source

"""
01_kafka_to_bronze_ingest.py

This notebook ingests real-time Kafka events using Structured Streaming,
parses the payload, and writes it to a Delta Bronze table.

Steps:
- Configure Kafka connection
- Read Kafka topic as streaming source
- Parse JSON payloads
- Write to Delta Bronze table with checkpointing

Used in: Kafka → Delta Bronze → Silver → MLflow pipeline
"""

# Step 1: Kafka connection and stream options

# In production, load secrets via Databricks Secrets or .env
kafka_bootstrap = "pkc-zgp5j7.us-south1.gcp.confluent.cloud:9092"
kafka_topic = "stream-input"
kafka_api_key = "YOUR_KAFKA_API_KEY"
kafka_api_secret = "YOUR_KAFKA_API_SECRET"

# Kafka consumer options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'
}


# Step 2: Ingest Kafka stream as raw binary DataFrame
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

print("Raw Kafka schema:")
df_raw.printSchema()


# Step 3: Parse Kafka JSON payload

from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col

# Define expected schema of incoming event
event_schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("value", StringType())

# Extract and parse 'value' field as JSON
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

print("Parsed schema preview:")
df_parsed.printSchema()


# Step 4: Write parsed stream to Bronze Delta table

(
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kafka_checkpoint_bronze")  # In prod use DBFS location
    .table("bronze_events")
)


# Step 5: Stream monitoring (optional)

# You can view the streaming query status:
# spark.streams.active

# Or query the Bronze table in a SQL cell:
# %sql
# SELECT * FROM bronze_events ORDER BY timestamp DESC LIMIT 20;
