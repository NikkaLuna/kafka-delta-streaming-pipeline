# Databricks notebook source

# COMMAND ----------
# Natural Language Operations Assistant

# COMMAND ----------

pip install -U mlflow

# COMMAND ----------

pip install openai

# COMMAND ----------

dbutils.library.restartPython()


# Databricks notebook source

# COMMAND ----------
# Natural Language Operations Assistant

# COMMAND ----------

!pip install openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from openai import OpenAI
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.services.ai.azure.com/openai/v1"
LLM_MODEL = "gpt-4.1-mini"

BUSINESS_IMPACT_TABLE = "business_impact_metrics"
REVIEW_TABLE = "human_review_queue"
RAG_TABLE = "gold_events_enriched_rag"

# COMMAND ----------

client = OpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    base_url=AZURE_OPENAI_ENDPOINT
)

# COMMAND ----------

business_df = spark.table(BUSINESS_IMPACT_TABLE)
review_df = spark.table(REVIEW_TABLE)
rag_df = spark.table(RAG_TABLE)

display(
    business_df.select(
        "event_id",
        "risk_level",
        "confidence",
        "estimated_revenue_at_risk",
        "affected_sessions_estimate",
        "priority_score",
        "recommended_escalation"
    )
)

display(
    review_df.select(
        "event_id",
        "review_status",
        "review_priority",
        "reviewer_notes",
        "reviewed_by"
    )
)

display(
    rag_df.select(
        "event_id",
        "related_incident",
        "recommended_action",
        "source_document"
    )
)

from openai import OpenAI
from pyspark.sql import functions as F

dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.services.ai.azure.com/openai/v1"
LLM_MODEL = "gpt-4.1-mini"

BUSINESS_IMPACT_TABLE = "business_impact_metrics"
REVIEW_TABLE = "human_review_queue"
RAG_TABLE = "gold_events_enriched_rag"

client = OpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    base_url=AZURE_OPENAI_ENDPOINT
)

business_df = spark.table(BUSINESS_IMPACT_TABLE)
review_df = spark.table(REVIEW_TABLE)
rag_df = spark.table(RAG_TABLE)

# COMMAND ----------

summary_rows = (
    business_df
    .select(
        "event_id",
        "risk_level",
        "confidence",
        "related_incident",
        "review_status",
        "estimated_revenue_at_risk",
        "affected_sessions_estimate",
        "priority_score",
        "recommended_escalation"
    )
    .collect()
)

ops_context = "\n".join(
    [
        f"""
Event ID: {row['event_id']}
Risk Level: {row['risk_level']}
Confidence: {row['confidence']}
Related Incident: {row['related_incident']}
Review Status: {row['review_status']}
Estimated Revenue at Risk: {row['estimated_revenue_at_risk']}
Affected Sessions Estimate: {row['affected_sessions_estimate']}
Priority Score: {row['priority_score']}
Recommended Escalation: {row['recommended_escalation']}
"""
        for row in summary_rows
    ]
)

print(ops_context)

# COMMAND ----------

def ask_ops_assistant(question: str):
    prompt = f"""
You are an operations intelligence assistant for a streaming anomaly detection platform.

Use the operational context below to answer the user's question.

Context:
{ops_context}

Question:
{question}

Rules:
- Be concise.
- Refer to specific event IDs when possible.
- Include business impact if available.
- Include recommended escalation if available.
- Do not invent events or metrics.
"""

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

    return response.choices[0].message.content

# COMMAND ----------

answer = ask_ops_assistant(
    "Which anomalies need attention and what action should operations take?"
)

print(answer)