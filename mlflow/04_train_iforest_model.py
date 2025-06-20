# Databricks notebook source
# MAGIC %pip install mlflow
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

# Step 0: Setup & Imports
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from pyspark.sql.functions import col, unix_timestamp
from pyspark.ml.feature import VectorAssembler


import pandas as pd
import mlflow
import mlflow.sklearn


# COMMAND ----------

# Step 1: Load the Silver Delta Table
df_silver = spark.read.table("silver_events")
df_silver = df_silver.select("event_id", "event_type", "timestamp", "value")

# COMMAND ----------

# Step 2: Feature Engineering

df_silver_numeric = (
    df_silver
    .withColumn("timestamp_unix", unix_timestamp("timestamp"))
    .withColumn("value", col("value").cast("double"))
)


assembler = VectorAssembler(
    inputCols=["value", "timestamp_unix"],
    outputCol="features"
)

df_features = assembler.transform(df_silver_numeric)

# COMMAND ----------

# Step 3: Feature Engineering + Isolation Forest (sklearn version)

from pyspark.sql.functions import col, unix_timestamp, expr
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from sklearn.ensemble import IsolationForest
import numpy as np

# 1. Prepare Spark DataFrame
df_silver_numeric = (
    df_silver
    .withColumn("timestamp_unix", unix_timestamp("timestamp"))
    .withColumn("value", col("value").cast("double"))
)

# 🔍 Optional: Check how many nulls exist
df_silver_numeric.selectExpr(
    "count(*) as total_rows",
    "sum(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_value",
    "sum(CASE WHEN timestamp_unix IS NULL THEN 1 ELSE 0 END) as null_timestamp"
).show()

# 2. Explicitly drop rows with nulls in key columns
df_clean = df_silver_numeric.dropna(subset=["value", "timestamp_unix"])

# 3. Assemble features
assembler = VectorAssembler(
    inputCols=["value", "timestamp_unix"],
    outputCol="features"
)
df_features = assembler.transform(df_clean)

# 4. Convert Spark vector column to Pandas DataFrame
df_pandas = df_features.select(
    vector_to_array("features").alias("features")
).toPandas()

# 5. Check if there’s anything to train on
if len(df_pandas) == 0:
    raise ValueError("No valid rows to train on after dropping nulls. Check your source data.")
else:
    # Convert to 2D numpy array
    X = np.array(df_pandas["features"].tolist())

    # 6. Train Isolation Forest
    model = IsolationForest(
        contamination=0.1,
        max_samples='auto',
        random_state=42
    )
    model.fit(X)



# COMMAND ----------

df_silver.show(5, truncate=False)


# COMMAND ----------

df_silver.printSchema()


# COMMAND ----------

# Step 4: Start MLflow Run & Log Model

with mlflow.start_run(run_name="silver_iforest_model") as run:
    
    input_example = X[0].reshape(1, -1)

    mlflow.sklearn.log_model(
        model,
        "model",
        input_example=input_example  # ✅ Now MLflow will log the schema!
    )

    mlflow.log_param("contamination", 0.1)
    mlflow.log_param("features", ["value", "timestamp_unix"])
    mlflow.log_metric("training_records", df_features.count())

    run_id = run.info.run_id




# COMMAND ----------

# Step 5: Register Model

mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name="iforest_silver_anomaly_detector"
)


# COMMAND ----------

spark.sql("DESCRIBE HISTORY silver_events").show(truncate=False)
