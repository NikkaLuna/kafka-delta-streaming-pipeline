# Databricks notebook source

# COMMAND ----------
# Business Impact Simulation

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_TABLE = "human_review_queue"
OUTPUT_TABLE = "business_impact_metrics"

# COMMAND ----------

review_df = spark.table(SOURCE_TABLE)

display(review_df)

# COMMAND ----------

impact_df = (
    review_df
    .withColumn(
        "estimated_revenue_at_risk",
        F.when(F.col("risk_level") == "high", F.lit(5000.0))
         .when(F.col("risk_level") == "medium", F.lit(1500.0))
         .otherwise(F.lit(250.0))
    )
    .withColumn(
        "affected_sessions_estimate",
        F.when(F.col("risk_level") == "high", F.lit(250))
         .when(F.col("risk_level") == "medium", F.lit(75))
         .otherwise(F.lit(10))
    )
    .withColumn(
        "priority_score",
        (
            F.col("confidence") * 100
            + F.when(F.col("risk_level") == "high", F.lit(50))
               .when(F.col("risk_level") == "medium", F.lit(25))
               .otherwise(F.lit(5))
        )
    )
    .withColumn(
        "recommended_escalation",
        F.when(
            (F.col("risk_level") == "high") | (F.col("priority_score") >= 100),
            F.lit("escalate_to_operations")
        )
        .when(
            F.col("risk_level") == "medium",
            F.lit("monitor_and_review")
        )
        .otherwise(F.lit("log_only"))
    )
    .withColumn(
        "impact_simulation_ts",
        F.current_timestamp()
    )
)

display(
    impact_df.select(
        "event_id",
        "risk_level",
        "confidence",
        "related_incident",
        "review_status",
        "estimated_revenue_at_risk",
        "affected_sessions_estimate",
        "priority_score",
        "recommended_escalation",
        "impact_simulation_ts"
    )
)

# COMMAND ----------

(
    impact_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Created {OUTPUT_TABLE}")

# COMMAND ----------

display(
    spark.table("business_impact_metrics")
    .select(
        "event_id",
        "risk_level",
        "confidence",
        "related_incident",
        "review_status",
        "estimated_revenue_at_risk",
        "affected_sessions_estimate",
        "priority_score",
        "recommended_escalation",
        "impact_simulation_ts"
    )
)