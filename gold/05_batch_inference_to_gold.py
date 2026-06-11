# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow.sklearn

from pyspark.sql.functions import unix_timestamp, current_timestamp, lit, col

# COMMAND ----------

MODEL_NAME = "iforest_silver_anomaly_detector"
MODEL_VERSION = "9"
GOLD_TABLE = "gold_anomaly_predictions"

# COMMAND ----------

# Load registered Isolation Forest model from MLflow Model Registry
model_uri = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
model = mlflow.sklearn.load_model(model_uri)

print(f"Loaded model: {model_uri}")

# COMMAND ----------

# Load Silver Delta table and prepare model features
df_silver = spark.read.table("silver_events")

df_features = (
    df_silver
    .withColumn("timestamp_unix", unix_timestamp("timestamp"))
    .withColumn("value", col("value").cast("double"))
    .dropna(subset=["value", "timestamp_unix"])
    .select("event_id", "event_type", "timestamp", "value", "timestamp_unix")
)

print(f"Rows available for scoring: {df_features.count()}")

# COMMAND ----------

# Convert to Pandas for sklearn inference
df_pd = df_features.toPandas()

X = df_pd[["value", "timestamp_unix"]].astype(float)

# COMMAND ----------

# Score events
df_pd["anomaly_score"] = model.decision_function(X)
df_pd["anomaly_flag"] = model.predict(X)

# Convention:
# -1 = anomaly
#  1 = normal

# COMMAND ----------

# Convert predictions back to Spark DataFrame
df_gold = spark.createDataFrame(df_pd)

df_gold = (
    df_gold
    .withColumn("inference_ts", current_timestamp())
    .withColumn("mlflow_model_name", lit(MODEL_NAME))
    .withColumn("mlflow_model_version", lit(MODEL_VERSION))
)

# COMMAND ----------

# Write scored events to Gold Delta table
(
    df_gold.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

display(df_gold.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top anomalies
# MAGIC SELECT *
# MAGIC FROM gold_anomaly_predictions
# MAGIC WHERE anomaly_flag = -1
# MAGIC ORDER BY anomaly_score ASC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly distribution
# MAGIC SELECT anomaly_flag, COUNT(*) AS count
# MAGIC FROM gold_anomaly_predictions
# MAGIC GROUP BY anomaly_flag

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily scoring volume
# MAGIC SELECT
# MAGIC   DATE(inference_ts) AS scoring_date,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(CASE WHEN anomaly_flag = -1 THEN 1 ELSE 0 END) AS anomaly_events
# MAGIC FROM gold_anomaly_predictions
# MAGIC GROUP BY DATE(inference_ts)
# MAGIC ORDER BY scoring_date DESC