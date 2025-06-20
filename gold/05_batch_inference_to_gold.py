# Databricks notebook source

# MAGIC 
%pip install mlflow


# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

# Step 1: Imports

import mlflow.sklearn
from pyspark.sql.functions import unix_timestamp
import pandas as pd


# COMMAND ----------

# Step 2: Load Model from MLflow Registry
mlflow.sklearn.load_model("models:/iforest_silver_anomaly_detector/9")

model = mlflow.sklearn.load_model("models:/iforest_silver_anomaly_detector/9")

# COMMAND ----------


# Step 3: Load Silver Delta Table & Prepare Features
df_silver = spark.read.table("silver_events")

df_silver_numeric = (
    df_silver
    .withColumn("timestamp_unix", unix_timestamp("timestamp"))
    .dropna(subset=["value", "timestamp_unix"])  # Just in case
)

# Convert to Pandas for sklearn
df_pd = df_silver_numeric.select("event_id", "event_type", "timestamp", "value", "timestamp_unix").toPandas()

# Features
X = df_pd[["value", "timestamp_unix"]].astype(float)

# COMMAND ----------

# Step 4: Predict Anomalies
df_pd["anomaly_score"] = model.decision_function(X)
df_pd["anomaly_flag"] = model.predict(X)

# Convention: -1 = anomaly, 1 = normal

# COMMAND ----------

# Step 5: Save to Gold Delta Table
df_gold = spark.createDataFrame(df_pd)

(
    df_gold.write
    .mode("overwrite")  # Change to append if running repeatedly
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_anomaly_predictions")
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly distribution
# MAGIC SELECT anomaly_flag, COUNT(*) AS count
# MAGIC FROM gold_anomaly_predictions
# MAGIC GROUP BY anomaly_flag
# MAGIC

# COMMAND ----------

# Histogram of anomaly scores
import matplotlib.pyplot as plt

df_pd["anomaly_score"].hist(bins=30)
plt.title("Distribution of Anomaly Scores")
plt.xlabel("Score")
plt.ylabel("Frequency")
plt.show()


# COMMAND ----------

with mlflow.start_run(run_name="silver_iforest_model") as run:
    mlflow.sklearn.log_model(model, "model", input_example=X[:5])
    mlflow.log_param("contamination", 0.1)
    mlflow.log_param("features", ["value", "timestamp_unix"])
    mlflow.log_metric("training_records", len(X))

    run_id = run.info.run_id



# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping stream: {stream.name}")
    stream.stop()
