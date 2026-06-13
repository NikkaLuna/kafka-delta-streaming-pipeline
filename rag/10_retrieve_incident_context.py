# Databricks notebook source

# COMMAND ----------
# Retrieve Incident Context

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

ANOMALY_TABLE = "gold_anomaly_predictions"
KNOWLEDGE_TABLE = "knowledge_chunks"

# COMMAND ----------

anomaly_df = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("anomaly_flag") == -1)
    .filter(F.col("value") >= 60)
    .limit(1)
)

display(anomaly_df)

# COMMAND ----------

anomaly = anomaly_df.collect()[0]

event_type = str(anomaly["event_type"]).lower()
value = float(anomaly["value"])
anomaly_flag = int(anomaly["anomaly_flag"])

print("event_type:", event_type)
print("value:", value)
print("anomaly_flag:", anomaly_flag)

# COMMAND ----------

def build_retrieval_query(event_type, value, anomaly_flag):
    query_terms = [event_type]

    if anomaly_flag == -1:
        query_terms.append("anomaly")

    if value >= 60:
        query_terms.extend(
            [
                "high",
                "suspicious",
                "fraud",
                "repeated"
            ]
        )

    return query_terms

query_terms = build_retrieval_query(
    event_type,
    value,
    anomaly_flag
)

print(query_terms)

# COMMAND ----------

retrieval_df = spark.table(KNOWLEDGE_TABLE)

condition = None

for term in query_terms:
    term_condition = F.lower(F.col("chunk_text")).contains(term)

    if condition is None:
        condition = term_condition
    else:
        condition = condition | term_condition

retrieved_chunks = retrieval_df.filter(condition)

display(retrieved_chunks)

# COMMAND ----------

ranked_chunks = (
    retrieved_chunks
    .withColumn(
        "doc_rank",
        F.row_number().over(
            Window.partitionBy("source_doc").orderBy("chunk_index")
        )
    )
    .filter(F.col("doc_rank") == 1)
)

context_rows = (
    ranked_chunks
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

