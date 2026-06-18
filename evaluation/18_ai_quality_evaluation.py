# Databricks notebook source

# COMMAND ----------

%pip install -U langchain langchain-core langchain-openai pydantic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import AzureChatOpenAI

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType, TimestampType
)


# COMMAND ----------

dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.openai.azure.com"
LLM_MODEL = "gpt-4.1-mini"

SOURCE_TABLE = "gold_events_enriched_rag"
OUTPUT_TABLE = "ai_quality_eval_metrics"
EVAL_VERSION = "v1.0-llm-as-judge"

# COMMAND ----------

class AIQualityEvaluation(BaseModel):
    relevance_score: int = Field(ge=1, le=5)
    groundedness_score: int = Field(ge=1, le=5)
    completeness_score: int = Field(ge=1, le=5)
    hallucination_risk: Literal["low", "medium", "high"]
    hallucination_detected: bool
    evaluation_summary: str
    recommended_improvement: str

parser = PydanticOutputParser(pydantic_object=AIQualityEvaluation)

# COMMAND ----------

prompt_template = ChatPromptTemplate.from_messages([
    ("system", """
You are an AI quality evaluator reviewing RAG-based anomaly analysis.

Evaluate whether the AI output is relevant, grounded in the retrieved context, complete, and free from hallucinated claims.

Scoring:
1 = poor
2 = weak
3 = acceptable
4 = strong
5 = excellent

Return only structured output matching the requested schema.
"""),
    ("user", """
Event summary:
{event_summary}

Risk explanation:
{risk_explanation}

Related incident:
{related_incident}

Recommended action:
{recommended_action}

Source document:
{source_document}

Retrieved RAG context:
{rag_context}

Raw model response:
{raw_response}

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

print("AI quality evaluation chain created")

# COMMAND ----------

source_df = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("structured_output_valid") == True)
    .orderBy(F.col("enrichment_ts").desc())
    .limit(5)
)

display(source_df)

# COMMAND ----------

rows = [row.asDict() for row in source_df.collect()]
eval_rows = []

for row in rows:
    evaluation = chain.invoke({
        "event_summary": row.get("event_summary", ""),
        "risk_explanation": row.get("risk_explanation", ""),
        "related_incident": row.get("related_incident", ""),
        "recommended_action": row.get("recommended_action", ""),
        "source_document": row.get("source_document", ""),
        "rag_context": row.get("rag_context", ""),
        "raw_response": row.get("raw_response", ""),
        "format_instructions": parser.get_format_instructions()
    })

    overall_score = round(
        (
            evaluation.relevance_score
            + evaluation.groundedness_score
            + evaluation.completeness_score
        ) / 3,
        2
    )

    eval_rows.append(Row(
        event_id=str(row.get("event_id")),
        source_document=str(row.get("source_document")),
        related_incident=str(row.get("related_incident")),
        risk_level=str(row.get("risk_level")),

        relevance_score=int(evaluation.relevance_score),
        groundedness_score=int(evaluation.groundedness_score),
        completeness_score=int(evaluation.completeness_score),
        overall_quality_score=float(overall_score),

        hallucination_risk=evaluation.hallucination_risk,
        hallucination_detected=bool(evaluation.hallucination_detected),
        evaluation_summary=evaluation.evaluation_summary,
        recommended_improvement=evaluation.recommended_improvement,

        judge_model=LLM_MODEL,
        eval_version=EVAL_VERSION,
        evaluation_ts=datetime.utcnow()
    ))

print(f"Evaluated {len(eval_rows)} RAG-enriched outputs")

# COMMAND ----------

eval_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("source_document", StringType(), True),
    StructField("related_incident", StringType(), True),
    StructField("risk_level", StringType(), True),

    StructField("relevance_score", IntegerType(), True),
    StructField("groundedness_score", IntegerType(), True),
    StructField("completeness_score", IntegerType(), True),
    StructField("overall_quality_score", DoubleType(), True),

    StructField("hallucination_risk", StringType(), True),
    StructField("hallucination_detected", BooleanType(), True),
    StructField("evaluation_summary", StringType(), True),
    StructField("recommended_improvement", StringType(), True),

    StructField("judge_model", StringType(), True),
    StructField("eval_version", StringType(), True),
    StructField("evaluation_ts", TimestampType(), True),
])

eval_df = spark.createDataFrame(eval_rows, schema=eval_schema)

display(eval_df)

# COMMAND ----------

(
    eval_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Created table: {OUTPUT_TABLE}")

# COMMAND ----------

display(
    spark.table(OUTPUT_TABLE)
    .select(
        "event_id",
        "source_document",
        "relevance_score",
        "groundedness_score",
        "completeness_score",
        "overall_quality_score",
        "hallucination_risk",
        "hallucination_detected"
    )
    .orderBy(F.col("overall_quality_score").desc())
)

# COMMAND ----------

display(
    spark.table(OUTPUT_TABLE)
    .agg(
        F.count("*").alias("evaluated_outputs"),
        F.round(F.avg("relevance_score"), 2).alias("avg_relevance_score"),
        F.round(F.avg("groundedness_score"), 2).alias("avg_groundedness_score"),
        F.round(F.avg("completeness_score"), 2).alias("avg_completeness_score"),
        F.round(F.avg("overall_quality_score"), 2).alias("avg_overall_quality_score"),
        F.sum(F.col("hallucination_detected").cast("int")).alias("hallucination_flags")
    )
)


