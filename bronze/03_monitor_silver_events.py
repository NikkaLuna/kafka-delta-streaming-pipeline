# Databricks notebook source

# -------------------------------------------
# Silver Table Stream Monitor with Delta Logging
#
# This notebook checks the `silver_events` Delta table for late or stale records,
# prints a result, and logs monitoring metrics to the `monitor_logs` Delta table.
# -------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

# Load Silver Delta Table
df = spark.read.table("silver_events")

# Count events older than 5 minutes
late_count = df.filter(
    F.col("timestamp") < F.current_timestamp() - F.expr("INTERVAL 5 MINUTES")
).count()

# Print Monitoring Result
if late_count > 0:
    print(f"{late_count} late events detected in silver_events.")
    # TODO: Add alert integration (e.g. email, webhook, Slack)
else:
    print("No late events. Stream is healthy.")

# Log late_count to Delta for monitoring visibility
monitor_df = spark.createDataFrame([
    Row(run_time=datetime.utcnow(), late_events=late_count)
])

monitor_df.write.mode("append").format("delta").saveAsTable("monitor_logs")

# COMMAND ----------

# Optional Preview Query
# Run this cell to quickly inspect the logged monitoring metrics

# In notebook format use a SQL cell instead:
# %sql
# SELECT * FROM monitor_logs ORDER BY run_time DESC

