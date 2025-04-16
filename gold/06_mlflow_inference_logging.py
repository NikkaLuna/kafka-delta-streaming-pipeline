# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Step 0: Imports
# Step 0: Imports
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from pyspark.sql.functions import unix_timestamp, current_timestamp, lit, col, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
import pyspark.sql.functions as F


# COMMAND ----------

# Step 1: Load Silver Table (with dev fallback if recent data is empty)

from pyspark.sql.functions import col
import pyspark.sql.functions as F

# Try to load recent (last 1 day) events
df_silver = spark.read.table("silver_events").filter(
    col("timestamp") >= F.expr("current_timestamp() - INTERVAL 1 DAY")
)

# Check if it's empty — fallback to loading all or a sample
if df_silver.count() == 0:
    print("⚠️ No recent data found. Loading full table (dev mode).")
    df_silver = spark.read.table("silver_events").limit(10000)


# COMMAND ----------

# Step 2: Feature Engineering
df_silver_clean = (
    df_silver
    .withColumn("timestamp_unix", unix_timestamp("timestamp"))
    .withColumn("value", col("value").cast("double"))
    .dropna(subset=["value", "timestamp_unix"])
)

assembler = VectorAssembler(
    inputCols=["value", "timestamp_unix"],
    outputCol="features"
)

df_features = assembler.transform(df_silver_clean)

# COMMAND ----------

# Step 2.5: Convert features vector to array
df_features_array = df_features.withColumn("features_array", vector_to_array("features"))

# COMMAND ----------

print("Row count:", df_pandas.shape[0])
print(df_pandas.head())


# COMMAND ----------

# Step 3: Train Isolation Forest in memory (no MLflow model loading)
from sklearn.ensemble import IsolationForest

# Use Pandas DataFrame (assumes df_pandas already exists and is preprocessed)
X = df_pandas[["value", "timestamp_unix"]].values

model = IsolationForest(contamination=0.1, random_state=42)
model.fit(X)

# Run inference directly (no saving/loading)
df_pandas["anomaly_score"] = model.predict(X)

print("✅ Model trained and predictions added.")
df_pandas.head()





# COMMAND ----------

 #Step 5: Log Inference Metadata to MLflowimport mlflow

with mlflow.start_run(run_name="iforest_in_memory_batch") as run:
    mlflow.log_param("contamination", 0.1)
    mlflow.log_param("features", ["value", "timestamp_unix"])
    mlflow.log_metric("num_rows", len(df_pandas))
    mlflow.log_metric("avg_anomaly_score", float(df_pandas["anomaly_score"].mean()))

    # Save sample predictions
    sample = df_pandas[["value", "timestamp_unix", "anomaly_score"]].head(5)
    mlflow.log_dict(sample.to_dict(orient="records"), "sample_predictions.json")

    print("✅ Metrics and predictions logged to MLflow.")



# COMMAND ----------

# Step 6.5b: Confusion Matrix Visualization

import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

# Define threshold
threshold = -0.5

# Assign labels
df_pandas["predicted_label"] = (df_pandas["anomaly_score"] < threshold).astype(int)
df_pandas["true_label"] = 0  # demo purposes — replace with actual labels if available

# Confusion matrix
cm = confusion_matrix(df_pandas["true_label"], df_pandas["predicted_label"])
disp = ConfusionMatrixDisplay(confusion_matrix=cm)
disp.plot()
plt.title("Confusion Matrix")
plt.grid(False)
plt.tight_layout()
plt.savefig("confusion_matrix.png")

# Log to MLflow
mlflow.log_artifact("confusion_matrix.png")

print("✅ Confusion matrix saved and logged.")



# COMMAND ----------

# Step 6.5c: KDE Plot – Anomaly Score Distribution

import seaborn as sns

# KDE plot of anomaly score distribution
plt.figure(figsize=(8, 5))
sns.kdeplot(df_pandas["anomaly_score"], fill=True)
plt.axvline(threshold, color="red", linestyle="--", label=f"Threshold = {threshold}")
plt.title("Anomaly Score Distribution")
plt.xlabel("Anomaly Score")
plt.legend()
plt.tight_layout()
plt.savefig("score_distribution.png")

# Log to MLflow
mlflow.log_artifact("score_distribution.png")

print("✅ Score distribution plot saved and logged.")



# COMMAND ----------

print(df_pandas.columns)


# COMMAND ----------

# Step 7: Merge predictions with Silver context and write to Delta Gold 💾

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

# 1️⃣ Grab context columns from cleaned Silver data
df_expanded = df_silver_clean.select("event_id", "event_type", "timestamp", "value", "timestamp_unix")

# 2️⃣ Convert Pandas predictions back to Spark DataFrame
df_scored_pandas = spark.createDataFrame(df_pandas)

# 3️⃣ Join on timestamp_unix + value
df_joined = df_expanded.join(
    df_scored_pandas,
    on=["value", "timestamp_unix"],
    how="inner"
)

# 4️⃣ Add inference metadata columns
final_df = df_joined.withColumn("inference_ts", current_timestamp()) \
                    .withColumn("mlflow_run_id", lit(run.info.run_id))

# 5️⃣ Drop timestamp_unix to avoid Delta schema clash
final_df = final_df.drop("timestamp_unix")

# 6️⃣ 💥 Cast critical columns for Delta schema compatibility
final_df = final_df.withColumn("anomaly_score", final_df["anomaly_score"].cast(DoubleType()))
final_df = final_df.withColumn("predicted_label", final_df["predicted_label"].cast(IntegerType()))
final_df = final_df.withColumn("true_label", final_df["true_label"].cast(IntegerType()))

# 7️⃣ Write to Gold Delta table, partitioned by event_type
final_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("event_type") \
    .saveAsTable("gold_events_scored")

print("✅ Inference results written to Delta table: gold_events_scored")





# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 20 anomalies by score
# MAGIC SELECT *
# MAGIC FROM gold_events_scored
# MAGIC WHERE anomaly_score = -1
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count of anomalies vs normal predictions
# MAGIC SELECT
# MAGIC   anomaly_score,
# MAGIC   COUNT(*) AS count
# MAGIC FROM gold_events_scored
# MAGIC GROUP BY anomaly_score
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily anomaly trends
# MAGIC SELECT
# MAGIC   DATE(inference_ts) AS date,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(CASE WHEN anomaly_score = -1 THEN 1 ELSE 0 END) AS anomalies
# MAGIC FROM gold_events_scored
# MAGIC GROUP BY DATE(inference_ts)
# MAGIC ORDER BY date DESC
# MAGIC

# COMMAND ----------


display(
    scored_df.select(
        "event_id",
        "event_type",
        "value",
        "timestamp",
        "anomaly_score"
    )
)



# COMMAND ----------

# View partition-aware Delta Gold output
final_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("event_type") \
    .saveAsTable("gold_events_scored")



# COMMAND ----------


spark.sql("DESCRIBE HISTORY gold_events_scored").show(truncate=False)





# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_events_scored ORDER BY inference_ts DESC LIMIT 20
# MAGIC