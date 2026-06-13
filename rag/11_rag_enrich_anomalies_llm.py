# Databricks notebook source

# COMMAND ----------
# RAG-Enriched Anomaly Intelligence with Azure OpenAI

# COMMAND ----------

# MAGIC %pip install openai pydantic

# COMMAND ----------

import json
import re
from datetime import datetime
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError
from pyspark.sql import Row
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.services.ai.azure.com/openai/v1"
LLM_MODEL = "gpt-4.1-mini"
PROMPT_VERSION = "v1.0-rag-anomaly-context"

SOURCE_TABLE = "gold_anomaly_predictions"
KNOWLEDGE_TABLE = "knowledge_chunks"
OUTPUT_TABLE = "gold_events_enriched_rag"

# COMMAND ----------

client = OpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    base_url=AZURE_OPENAI_ENDPOINT
)

# COMMAND ----------

class RagEventInsight(BaseModel):
    event_summary: str
    user_intent: str
    risk_level: Literal["low", "medium", "high"]
    risk_explanation: str
    confidence: float = Field(ge=0.0, le=1.0)
    related_incident: str
    recommended_action: str
    source_document: str

# COMMAND ----------

def clean_json_response(raw_text: str) -> str:
    cleaned = raw_text.strip()

    if cleaned.startswith("```json"):
        cleaned = cleaned.replace("```json", "", 1).strip()

    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```", "", 1).strip()

    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()

    return cleaned

# COMMAND ----------

def retrieve_context(limit=3):
    retrieved_chunks = (
        spark.table(KNOWLEDGE_TABLE)
        .filter(F.lower(F.col("chunk_text")).contains("click"))
        .select("source_doc", "chunk_text")
        .limit(limit)
        .collect()
    )

    context = "\n\n".join(
        [
            f"Source: {row['source_doc']}\n{row['chunk_text']}"
            for row in retrieved_chunks
        ]
    )

    return context

# COMMAND ----------

def build_rag_prompt(event, retrieved_context):
    return f"""
You are an operational intelligence analyst reviewing anomalous clickstream events.

Use the anomaly event data and the retrieved operational context to produce a structured JSON response.

Important anomaly flag meaning:
- anomaly_flag = -1 means the event IS anomalous.
- anomaly_flag = 1 means the event is normal.

Event data:
event_id: {event['event_id']}
event_type: {event['event_type']}
timestamp: {event['timestamp']}
value: {event['value']}
anomaly_score: {event['anomaly_score']}
anomaly_flag: {event['anomaly_flag']}

Retrieved operational context:
{retrieved_context}

Return only valid JSON with these fields:
- event_summary
- user_intent
- risk_level
- risk_explanation
- confidence
- related_incident
- recommended_action
- source_document

Rules:
- risk_level must be one of: low, medium, high.
- confidence must be between 0.0 and 1.0.
- related_incident should name the most relevant retrieved incident or playbook.
- source_document should match one of the retrieved source document names.
- Do not include markdown fences.
- Do not include explanatory text outside the JSON object.
"""

# COMMAND ----------

test_event = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("anomaly_flag") == -1)
    .limit(1)
    .collect()[0]
)

retrieved_context = retrieve_context(limit=3)
prompt = build_rag_prompt(test_event, retrieved_context)

print(prompt)

# COMMAND ----------

response = client.chat.completions.create(
    model=LLM_MODEL,
    messages=[
        {
            "role": "user",
            "content": prompt
        }
    ],
    temperature=0
)

raw_response = response.choices[0].message.content

print(raw_response)

# COMMAND ----------

cleaned_response = clean_json_response(raw_response)

print(cleaned_response)

# COMMAND ----------

validated_output = RagEventInsight.parse_raw(cleaned_response)

print(validated_output)

# COMMAND ----------

rag_record = Row(
    event_id=str(test_event["event_id"]),
    event_type=str(test_event["event_type"]),
    timestamp=test_event["timestamp"],
    value=float(test_event["value"]),
    anomaly_score=float(test_event["anomaly_score"]),
    anomaly_flag=int(test_event["anomaly_flag"]),

    event_summary=str(validated_output.event_summary),
    user_intent=str(validated_output.user_intent),
    risk_level=str(validated_output.risk_level),
    risk_explanation=str(validated_output.risk_explanation),
    confidence=float(validated_output.confidence),
    related_incident=str(validated_output.related_incident),
    recommended_action=str(validated_output.recommended_action),
    source_document=str(validated_output.source_document),

    llm_model=str(LLM_MODEL),
    prompt_version=str(PROMPT_VERSION),
    rag_context=str(retrieved_context),
    raw_response=str(raw_response),
    structured_output_valid=True,
    error_message="",
    enrichment_ts=datetime.utcnow()
)

rag_df = spark.createDataFrame([rag_record])

display(rag_df)

# COMMAND ----------

(
    rag_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(OUTPUT_TABLE)
)

# COMMAND ----------

display(
    spark.table("gold_events_enriched_rag")
)