# Databricks notebook source

# COMMAND ----------
%pip install -U langchain langchain-core langchain-openai pydantic
dbutils.library.restartPython()

# COMMAND ----------
from pydantic import BaseModel, Field
from typing import Literal
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import AzureChatOpenAI
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.openai.azure.com"
LLM_MODEL = "gpt-4.1-mini"
SOURCE_TABLE = "gold_anomaly_predictions"

# COMMAND ----------

class EventInsight(BaseModel):
    event_summary: str
    user_intent: str
    risk_level: Literal["low", "medium", "high"]
    risk_explanation: str
    confidence: float = Field(ge=0.0, le=1.0)

parser = PydanticOutputParser(pydantic_object=EventInsight)

# COMMAND ----------
prompt_template = ChatPromptTemplate.from_messages([
    ("system", """
You are an operational intelligence analyst reviewing anomalous clickstream events.

Important anomaly flag meaning:
- anomaly_flag = -1 means the event IS anomalous.
- anomaly_flag = 1 means the event is normal.

Return only structured output that matches the requested schema.
"""),
    ("user", """
Event data:
event_id: {event_id}
event_type: {event_type}
timestamp: {timestamp}
value: {value}
anomaly_score: {anomaly_score}
anomaly_flag: {anomaly_flag}

{format_instructions}
""")
])

# COMMAND ----------
llm = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    azure_deployment=LLM_MODEL,
    api_version="2024-10-21",
    temperature=0
)

chain = prompt_template | llm | parser
print("chain created")

# COMMAND ----------

SOURCE_TABLE = "gold_anomaly_predictions"

test_event = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("anomaly_flag") == -1)
    .limit(1)
    .collect()[0]
)

print(test_event)

# COMMAND ----------

validated_output = chain.invoke(
    {
        "event_id": test_event["event_id"],
        "event_type": test_event["event_type"],
        "timestamp": str(test_event["timestamp"]),
        "value": float(test_event["value"]),
        "anomaly_score": float(test_event["anomaly_score"]),
        "anomaly_flag": int(test_event["anomaly_flag"]),
        "format_instructions": parser.get_format_instructions()
    }
)

print(validated_output)