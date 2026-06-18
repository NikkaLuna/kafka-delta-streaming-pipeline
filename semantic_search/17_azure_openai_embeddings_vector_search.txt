# Databricks notebook source

# COMMAND ----------

%pip install "openai==1.86.0" "numpy<2"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
from openai import AzureOpenAI
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    ArrayType, FloatType, DoubleType
)

# COMMAND ----------

dbutils.widgets.text("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_API_KEY = dbutils.widgets.get("AZURE_OPENAI_API_KEY")

AZURE_OPENAI_ENDPOINT = "https://clickstream-ai-project-resource.openai.azure.com"
EMBEDDING_DEPLOYMENT = "text-embedding-3-small"

KNOWLEDGE_TABLE = "knowledge_chunks"
EMBEDDING_TABLE = "knowledge_chunk_embeddings"
VECTOR_SEARCH_RESULTS_TABLE = "semantic_search_embedding_results"

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version="2024-10-21"
)

# COMMAND ----------

def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(
        model=EMBEDDING_DEPLOYMENT,
        input=text
    )
    return response.data[0].embedding

def cosine_similarity(vec_a, vec_b) -> float:
    a = np.array(vec_a, dtype=float)
    b = np.array(vec_b, dtype=float)

    denominator = np.linalg.norm(a) * np.linalg.norm(b)

    if denominator == 0:
        return 0.0

    return float(np.dot(a, b) / denominator)

print("Embedding helpers created")

# COMMAND ----------

knowledge_df = (
    spark.table(KNOWLEDGE_TABLE)
    .select("chunk_id", "source_doc", "doc_type", "chunk_text")
)

display(knowledge_df)

# COMMAND ----------

knowledge_rows = knowledge_df.collect()

embedded_rows = []

for row in knowledge_rows:
    embedding = get_embedding(row["chunk_text"])

    embedded_rows.append(
        {
            "chunk_id": row["chunk_id"],
            "source_doc": row["source_doc"],
            "doc_type": row["doc_type"],
            "chunk_text": row["chunk_text"],
            "embedding": [float(x) for x in embedding]
        }
    )

print(f"Generated embeddings for {len(embedded_rows)} knowledge chunks")

# COMMAND ----------

embedding_schema = StructType(
    [
        StructField("chunk_id", StringType(), True),
        StructField("source_doc", StringType(), True),
        StructField("doc_type", StringType(), True),
        StructField("chunk_text", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), True)
    ]
)

embedding_df = spark.createDataFrame(embedded_rows, schema=embedding_schema)

display(
    embedding_df.select("chunk_id", "source_doc", "doc_type")
)

# COMMAND ----------

(
    embedding_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(EMBEDDING_TABLE)
)

print(f"Created table: {EMBEDDING_TABLE}")

# COMMAND ----------

query_text = "suspicious high value click activity with possible fraud"

query_embedding = get_embedding(query_text)

embedded_chunks = spark.table(EMBEDDING_TABLE).collect()

similarity_rows = []

for row in embedded_chunks:
    score = cosine_similarity(query_embedding, row["embedding"])

    similarity_rows.append(
        {
            "query_text": query_text,
            "chunk_id": row["chunk_id"],
            "source_doc": row["source_doc"],
            "doc_type": row["doc_type"],
            "chunk_text": row["chunk_text"],
            "cosine_similarity": score
        }
    )

similarity_schema = StructType(
    [
        StructField("query_text", StringType(), True),
        StructField("chunk_id", StringType(), True),
        StructField("source_doc", StringType(), True),
        StructField("doc_type", StringType(), True),
        StructField("chunk_text", StringType(), True),
        StructField("cosine_similarity", DoubleType(), True)
    ]
)

similarity_df = spark.createDataFrame(similarity_rows, schema=similarity_schema)

ranked_similarity_df = (
    similarity_df
    .orderBy(F.col("cosine_similarity").desc())
)

display(
    ranked_similarity_df.select(
        "chunk_id",
        "source_doc",
        "doc_type",
        "cosine_similarity"
    )
)

# COMMAND ----------

(
    ranked_similarity_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(VECTOR_SEARCH_RESULTS_TABLE)
)

print(f"Created table: {VECTOR_SEARCH_RESULTS_TABLE}")

# COMMAND ----------

display(
    spark.table(VECTOR_SEARCH_RESULTS_TABLE)
    .select(
        "query_text",
        "chunk_id",
        "source_doc",
        "doc_type",
        "cosine_similarity"
    )
    .orderBy(F.col("cosine_similarity").desc())
)
