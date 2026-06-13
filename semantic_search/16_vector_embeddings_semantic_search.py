# Databricks notebook source

# COMMAND ----------
# Vector Embeddings / Semantic Search Baseline

from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer
from pyspark.ml.functions import vector_to_array

# COMMAND ----------

KNOWLEDGE_TABLE = "knowledge_chunks"
OUTPUT_TABLE = "knowledge_chunks_vectorized"

# COMMAND ----------

knowledge_df = spark.table(KNOWLEDGE_TABLE)

display(
    knowledge_df.select(
        "chunk_id",
        "source_doc",
        "doc_type",
        "chunk_text"
    )
)

# COMMAND ----------
# Tokenize text

tokenizer = Tokenizer(
    inputCol="chunk_text",
    outputCol="tokens"
)

tokenized_df = tokenizer.transform(knowledge_df)

# COMMAND ----------
# Convert tokens to TF vectors

hashing_tf = HashingTF(
    inputCol="tokens",
    outputCol="raw_features",
    numFeatures=256
)

tf_df = hashing_tf.transform(tokenized_df)

# COMMAND ----------
# Apply IDF weighting

idf = IDF(
    inputCol="raw_features",
    outputCol="features"
)

idf_model = idf.fit(tf_df)
vectorized_df = idf_model.transform(tf_df)

# COMMAND ----------
# Normalize vectors for cosine similarity

normalizer = Normalizer(
    inputCol="features",
    outputCol="normalized_features",
    p=2.0
)

normalized_df = normalizer.transform(vectorized_df)

vector_output_df = (
    normalized_df
    .select(
        "chunk_id",
        "source_doc",
        "doc_type",
        "chunk_index",
        "chunk_text",
        "normalized_features"
    )
)

display(vector_output_df)

# COMMAND ----------
# Save vectorized knowledge table

(
    vector_output_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"Created {OUTPUT_TABLE}")

# COMMAND ----------
# Build a vector for a natural-language query

query_text = "suspicious high value click activity with possible fraud"

query_df = spark.createDataFrame(
    [(query_text,)],
    ["chunk_text"]
)

query_tokens = tokenizer.transform(query_df)
query_tf = hashing_tf.transform(query_tokens)
query_vectorized = idf_model.transform(query_tf)
query_normalized = normalizer.transform(query_vectorized)

query_vector = (
    query_normalized
    .select("normalized_features")
    .collect()[0]["normalized_features"]
)

# COMMAND ----------
# Compute cosine similarity against knowledge chunks

query_array = query_vector.toArray().tolist()

query_array_df = spark.createDataFrame(
    [(query_array,)],
    ["query_vector"]
)

chunks_with_arrays = (
    vector_output_df
    .withColumn(
        "chunk_vector",
        vector_to_array("normalized_features")
    )
)

query_cross_df = chunks_with_arrays.crossJoin(query_array_df)

similarity_df = (
    query_cross_df
    .withColumn(
        "cosine_similarity",
        F.expr("""
            aggregate(
                zip_with(chunk_vector, query_vector, (x, y) -> x * y),
                cast(0.0 as double),
                (acc, x) -> acc + x
            )
        """)
    )
    .select(
        "chunk_id",
        "source_doc",
        "doc_type",
        "chunk_text",
        "cosine_similarity"
    )
    .orderBy(F.desc("cosine_similarity"))
)

display(similarity_df)