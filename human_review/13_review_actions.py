# Databricks notebook source

# COMMAND ----------
# Human Review Actions

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

SOURCE_TABLE = "human_review_queue"
OUTPUT_TABLE = "human_review_queue"

REVIEWER = "portfolio_demo_analyst"

# COMMAND ----------

queue_df = spark.table(SOURCE_TABLE)

display(queue_df)

# COMMAND ----------

pending_df = (
    queue_df
    .filter(F.col("review_status") == "pending")
)

display(pending_df)

# COMMAND ----------
# Simulate analyst review decision

reviewed_df = (
    queue_df
    .withColumn(
        "review_status",
        F.when(
            (F.col("review_status") == "pending") &
            (F.col("risk_level") == "high"),
            "escalated"
        )
        .when(
            (F.col("review_status") == "pending") &
            (F.col("risk_level") == "medium"),
            "approved"
        )
        .otherwise(F.col("review_status"))
    )
    .withColumn(
        "reviewer_notes",
        F.when(
            F.col("risk_level") == "high",
            "Escalated due to high AI-assessed risk and related incident context."
        )
        .when(
            F.col("risk_level") == "medium",
            "Approved for monitoring. AI recommendation reviewed and accepted."
        )
        .otherwise(F.col("reviewer_notes"))
    )
    .withColumn(
        "reviewed_by",
        F.when(
            F.col("review_status").isin("approved", "escalated"),
            REVIEWER
        ).otherwise(F.col("reviewed_by"))
    )
    .withColumn(
        "reviewed_ts",
        F.when(
            F.col("review_status").isin("approved", "escalated"),
            F.current_timestamp()
        ).otherwise(F.col("reviewed_ts"))
    )
)

display(reviewed_df)

# COMMAND ----------
# Overwrite review queue with updated review decisions

(
    reviewed_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Updated review decisions in {OUTPUT_TABLE}")

# COMMAND ----------
# Verify final review queue

display(
    spark.table(OUTPUT_TABLE)
    .select(
        "review_id",
        "event_id",
        "risk_level",
        "confidence",
        "related_incident",
        "recommended_action",
        "review_status",
        "review_priority",
        "reviewer_notes",
        "reviewed_by",
        "review_created_ts",
        "reviewed_ts"
    )
)