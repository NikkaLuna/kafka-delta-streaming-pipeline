# Databricks notebook source

# COMMAND ----------
# Build RAG Knowledge Base

from pyspark.sql import Row
from datetime import datetime
import os

# COMMAND ----------

KNOWLEDGE_BASE_PATH = "knowledge_base"
OUTPUT_TABLE = "knowledge_chunks"

# COMMAND ----------

docs = [
    {
        "source_doc": "payment_processor_outage_2024.md",
        "doc_type": "incident",
        "local_path": "knowledge_base/incidents/payment_processor_outage_2024.md"
    },
    {
        "source_doc": "cdn_latency_incident_2024.md",
        "doc_type": "incident",
        "local_path": "knowledge_base/incidents/cdn_latency_incident_2024.md"
    },
    {
        "source_doc": "checkout_abandonment_playbook.md",
        "doc_type": "playbook",
        "local_path": "knowledge_base/playbooks/checkout_abandonment_playbook.md"
    },
    {
        "source_doc": "fraud_response_playbook.md",
        "doc_type": "playbook",
        "local_path": "knowledge_base/playbooks/fraud_response_playbook.md"
    }
]

# COMMAND ----------

def read_local_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def chunk_text(text, max_chars=900):
    sections = [section.strip() for section in text.split("\n\n") if section.strip()]

    chunks = []
    current_chunk = ""

    for section in sections:
        if len(current_chunk) + len(section) <= max_chars:
            current_chunk += "\n\n" + section if current_chunk else section
        else:
            chunks.append(current_chunk)
            current_chunk = section

    if current_chunk:
        chunks.append(current_chunk)

    return chunks

# COMMAND ----------

rows = []

for doc in docs:
    text = read_local_file(doc["local_path"])
    chunks = chunk_text(text)

    for idx, chunk in enumerate(chunks):
        rows.append(
            Row(
                chunk_id=f"{doc['source_doc'].replace('.md', '')}_{idx}",
                source_doc=doc["source_doc"],
                doc_type=doc["doc_type"],
                chunk_index=idx,
                chunk_text=chunk,
                created_at=datetime.utcnow()
            )
        )

knowledge_chunks_df = spark.createDataFrame(rows)

display(knowledge_chunks_df)

# COMMAND ----------

(
    knowledge_chunks_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Wrote {knowledge_chunks_df.count()} chunks to {OUTPUT_TABLE}")

# COMMAND ----------

display(
    spark.table("knowledge_chunks")
    .select("chunk_id", "source_doc", "doc_type", "chunk_index", "chunk_text")
)