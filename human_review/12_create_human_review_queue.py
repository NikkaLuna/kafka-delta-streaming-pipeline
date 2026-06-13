# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

# COMMAND ----------

SOURCE_TABLE = "gold_events_enriched_rag"
OUTPUT_TABLE = "human_review_queue"

# COMMAND ----------

df = spark.table(SOURCE_TABLE)

display(df)

# COMMAND ----------

review_df = (
    df
    .filter(
        F.col("risk_level").isin(
            "medium",
            "high"
        )
    )
)

display(review_df)

# COMMAND ----------

queue_df = (
    review_df
    .withColumn(
        "review_id",
        F.expr("uuid()")
    )
    .withColumn(
        "review_status",
        F.lit("pending")
    )
    .withColumn(
        "review_priority",
        F.when(
            F.col("risk_level") == "high",
            "urgent"
        ).otherwise("normal")
    )
    .withColumn(
        "reviewer_notes",
        F.lit("")
    )
    .withColumn(
        "reviewed_by",
        F.lit("")
    )
    .withColumn(
        "review_created_ts",
        F.current_timestamp()
    )
    .withColumn(
        "reviewed_ts",
        F.lit(None).cast("timestamp")
    )
)

display(queue_df)

# COMMAND ----------

(
    queue_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Created {OUTPUT_TABLE}")

# COMMAND ----------

display(
    spark.table("human_review_queue")
)

