# Databricks notebook source

# COMMAND ----------
# Retrieve Incident Context

from pyspark.sql import functions as F

# COMMAND ----------

ANOMALY_TABLE = "gold_anomaly_predictions"
KNOWLEDGE_TABLE = "knowledge_chunks"

# COMMAND ----------

anomaly_df = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("anomaly_flag") == -1)
    .limit(1)
)

display(anomaly_df)

# COMMAND ----------

anomaly = anomaly_df.collect()[0]

event_type = anomaly["event_type"]
value = anomaly["value"]

print("event_type:", event_type)
print("value:", value)

# COMMAND ----------

retrieved_chunks = (
    spark.table(KNOWLEDGE_TABLE)
    .filter(
        F.lower(F.col("chunk_text")).contains("click")
    )
)

display(retrieved_chunks)

# COMMAND ----------

context_rows = (
    retrieved_chunks
    .select("source_doc", "chunk_text")
    .limit(3)
    .collect()
)

context = "\n\n".join(
    [
        f"Source: {row['source_doc']}\n{row['chunk_text']}"
        for row in context_rows
    ]
)

print(context)