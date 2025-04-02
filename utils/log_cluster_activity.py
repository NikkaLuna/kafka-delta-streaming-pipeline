# utils/log_cluster_activity.py
# ---------------------------------------------
# Cluster Usage Logger (Manual Simulation)
#
# This script simulates logging cluster usage for a specific Databricks job task.
# It appends entries to a Delta table (`cluster_logs`) with metadata like
# task name, cluster name, and runtime window.
# ---------------------------------------------

from pyspark.sql import Row
from datetime import datetime

# Simulated usage record (manual entry for tracking or demo purposes)
cluster_log = [
    Row(
        task_name="monitor_silver_events",
        cluster_name="kafka-streaming-cluster",
        start_time=datetime(2025, 4, 2, 15, 30),
        end_time=datetime.utcnow(),
        notes="Scheduled run with spot + auto-termination"
    )
]

# Convert to Spark DataFrame
df_cluster_log = spark.createDataFrame(cluster_log)

# Append to Delta table (create if doesn't exist)
df_cluster_log.write.mode("append").format("delta").saveAsTable("cluster_logs")

# ---------------------------------------------
# Optional: View cluster logs directly (in notebook or %sql cell)
# %sql
# SELECT * FROM cluster_logs ORDER BY end_time DESC
# ---------------------------------------------



