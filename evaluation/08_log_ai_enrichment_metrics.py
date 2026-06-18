# Databricks notebook source

# COMMAND ----------
# AI Enrichment Evaluation + MLflow Prompt Tracking

import time
import json
import mlflow
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Row

# COMMAND ----------

SOURCE_TABLE = "gold_events_enriched"
EVAL_TABLE = "ai_enrichment_eval_metrics"

EXPERIMENT_NAME = "ai-enrichment-evaluation"

LLM_MODEL = "gpt-4.1-mini"
PROMPT_VERSION = "v1.0-anomaly-risk-json"
NOTES = "Evaluation run for Azure OpenAI structured anomaly enrichment outputs."

# COMMAND ----------

df = spark.table(SOURCE_TABLE)

display(df.limit(5))

# COMMAND ----------

eval_df = (
    df.withColumn(
        "is_valid_structured_output",
        (
            F.col("event_summary").isNotNull()
            & F.col("user_intent").isNotNull()
            & F.col("risk_level").isin("low", "medium", "high")
            & F.col("risk_explanation").isNotNull()
            & F.col("confidence").between(0.0, 1.0)
        ).cast("int")
    )
)

display(eval_df)

# COMMAND ----------

start_time = time.time()

metrics_row = (
    eval_df
    .agg(
        F.count("*").alias("total_rows"),
        F.sum("is_valid_structured_output").alias("valid_outputs"),
        (F.count("*") - F.sum("is_valid_structured_output")).alias("invalid_outputs"),
        ((F.sum("is_valid_structured_output") / F.count("*")) * 100).alias("structured_output_validity_pct"),
        F.avg("confidence").alias("avg_confidence"),
        F.min("confidence").alias("min_confidence"),
        F.max("confidence").alias("max_confidence")
    )
    .collect()[0]
)

total_runtime_seconds = time.time() - start_time

metrics = {
    "total_rows": int(metrics_row["total_rows"] or 0),
    "valid_outputs": int(metrics_row["valid_outputs"] or 0),
    "invalid_outputs": int(metrics_row["invalid_outputs"] or 0),
    "structured_output_validity_pct": float(metrics_row["structured_output_validity_pct"] or 0.0),
    "avg_confidence": float(metrics_row["avg_confidence"] or 0.0),
    "min_confidence": float(metrics_row["min_confidence"] or 0.0),
    "max_confidence": float(metrics_row["max_confidence"] or 0.0),
    "total_runtime_seconds": float(total_runtime_seconds)
}

metrics

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = "/".join(notebook_path.split("/")[:-1])

EXPERIMENT_NAME = f"{notebook_dir}/ai-enrichment-evaluation"

print(EXPERIMENT_NAME)

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name="ai_enrichment_eval") as run:
    run_id = run.info.run_id

    mlflow.log_param("source_table", SOURCE_TABLE)
    mlflow.log_param("eval_table", EVAL_TABLE)
    mlflow.log_param("llm_model", LLM_MODEL)
    mlflow.log_param("prompt_version", PROMPT_VERSION)

    for key, value in metrics.items():
        mlflow.log_metric(key, value)

    mlflow.set_tag("pipeline_stage", "ai_enrichment")
    mlflow.set_tag("project", "kafka_delta_azure_openai_pipeline")
    mlflow.set_tag("evaluation_type", "structured_output_validation")

    mlflow.log_text(
        json.dumps(metrics, indent=2),
        artifact_file="ai_enrichment_eval_metrics.json"
    )

print(f"Logged MLflow run_id: {run_id}")

from pyspark.sql import Row
from datetime import datetime

eval_record = Row(
    run_id=run_id,
    run_timestamp=datetime.utcnow(),
    llm_model=LLM_MODEL,
    prompt_version=PROMPT_VERSION,
    total_rows=metrics["total_rows"],
    valid_outputs=metrics["valid_outputs"],
    invalid_outputs=metrics["invalid_outputs"],
    structured_output_validity_pct=metrics["structured_output_validity_pct"],
    avg_confidence=metrics["avg_confidence"],
    min_confidence=metrics["min_confidence"],
    max_confidence=metrics["max_confidence"],
    total_runtime_seconds=metrics["total_runtime_seconds"],
    notes=NOTES
)

eval_metrics_df = spark.createDataFrame([eval_record])

display(eval_metrics_df)

(
    eval_metrics_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("ai_enrichment_eval_metrics")
)

display(
    spark.table("ai_enrichment_eval_metrics")
)

screenshot_df = (
    spark.table("ai_enrichment_eval_metrics")
    .select(
        "run_timestamp",
        "llm_model",
        "total_rows",
        "valid_outputs",
        "structured_output_validity_pct",
        "avg_confidence"
    )
)

display(screenshot_df)