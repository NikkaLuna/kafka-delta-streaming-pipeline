# Databricks notebook source
# COMMAND ----------

# MAGIC %pip install openai pydantic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from openai import OpenAI

# API key is entered through a Databricks widget at runtime.
# Do not hardcode or commit API keys.
dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")

AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.services.ai.azure.com/openai/v1"
AZURE_OPENAI_DEPLOYMENT = "gpt-4.1-mini"

client = OpenAI(
    base_url=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY
)

# COMMAND ----------

# Connectivity test
response = client.responses.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    input="Respond with one sentence: Databricks can reach Azure OpenAI."
)

print(response.output_text)

# COMMAND ----------

spark.read.table("gold_anomaly_predictions") \
    .filter("anomaly_flag = -1") \
    .orderBy("anomaly_score") \
    .limit(5) \
    .show(truncate=False)

# COMMAND ----------

import json
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)


# Structured output schema

class EventInsight(BaseModel):
    event_summary: str
    user_intent: str
    risk_level: Literal["low", "medium", "high"]
    risk_explanation: str
    confidence: float = Field(ge=0.0, le=1.0)


# Prompt

SYSTEM_PROMPT = """
You are a clickstream anomaly analyst.

Important:
In this system, anomaly_flag = -1 means the event IS anomalous.
anomaly_flag = 1 means the event is normal.

Analyze the anomaly event and return only valid JSON with:
- event_summary
- user_intent
- risk_level: low, medium, or high
- risk_explanation
- confidence between 0 and 1

Do not wrap the JSON in markdown fences.
Return raw JSON only.
"""


# Helpers

def clean_json_response(raw_text: str) -> str:
    cleaned = raw_text.strip()

    if cleaned.startswith("```json"):
        cleaned = cleaned.replace("```json", "", 1).strip()

    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```", "", 1).strip()

    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()

    return cleaned


def enrich_event(event):
    user_prompt = f"""
Analyze this clickstream anomaly:

event_id: {event["event_id"]}
event_type: {event["event_type"]}
timestamp: {event["timestamp"]}
value: {event["value"]}
anomaly_score: {event["anomaly_score"]}
anomaly_flag: {event["anomaly_flag"]}

Remember:
anomaly_flag = -1 means this event is anomalous.
"""

    response = client.responses.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
    )

    raw_text = response.output_text
    json_text = clean_json_response(raw_text)

    try:
        parsed = EventInsight.parse_raw(json_text)
        result = parsed.dict()
        result["structured_output_valid"] = True
        result["error_message"] = None
    except Exception as e:
        result = {
            "event_summary": None,
            "user_intent": None,
            "risk_level": None,
            "risk_explanation": None,
            "confidence": None,
            "structured_output_valid": False,
            "error_message": str(e)
        }

    result["raw_response"] = raw_text
    result["event_id"] = event["event_id"]
    result["event_type"] = event["event_type"]
    result["timestamp"] = event["timestamp"]
    result["value"] = float(event["value"])
    result["anomaly_score"] = float(event["anomaly_score"])
    result["anomaly_flag"] = int(event["anomaly_flag"])
    result["llm_model"] = AZURE_OPENAI_DEPLOYMENT
    result["prompt_version"] = "v1.1"
    result["enrichment_ts"] = datetime.utcnow()

    return Row(**result)


# Load anomaly events

df_anomalies = (
    spark.read.table("gold_anomaly_predictions")
    .filter("anomaly_flag = -1")
    .orderBy("anomaly_score")
    .limit(5)
)

events = [row.asDict() for row in df_anomalies.collect()]

print(f"Loaded {len(events)} anomaly events for enrichment.")


# Enrich events

enriched_rows = [enrich_event(event) for event in events]

print(f"Enriched {len(enriched_rows)} anomaly events.")


# Explicit Spark schema

enriched_schema = StructType([
    StructField("event_summary", StringType(), True),
    StructField("user_intent", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("risk_explanation", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("structured_output_valid", BooleanType(), True),
    StructField("error_message", StringType(), True),
    StructField("raw_response", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True),
    StructField("anomaly_score", DoubleType(), True),
    StructField("anomaly_flag", IntegerType(), True),
    StructField("llm_model", StringType(), True),
    StructField("prompt_version", StringType(), True),
    StructField("enrichment_ts", TimestampType(), True),
])

df_enriched = spark.createDataFrame(enriched_rows, schema=enriched_schema)


# Write to Delta

(
    df_enriched.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("gold_events_enriched")
)

display(df_enriched)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN structured_output_valid = true THEN 1 ELSE 0 END) AS valid_outputs,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN structured_output_valid = true THEN 1 ELSE 0 END) / COUNT(*) * 100,
# MAGIC     2
# MAGIC   ) AS structured_output_validity_pct
# MAGIC FROM gold_events_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_events_enriched
# MAGIC ORDER BY enrichment_ts DESC
# MAGIC LIMIT 20;