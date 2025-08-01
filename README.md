# Kafka → Delta Lake Streaming Pipeline (Databricks)

A real-time, cloud-native data engineering pipeline that ingests, processes, and stores structured event data using Kafka, PySpark, and Delta Lake — purpose-built for portfolio demonstration. 

This project aligns with Databricks Certified Data Engineer Professional objectives.


## Tech Stack

![Kafka](https://img.shields.io/badge/Kafka-Confluent-orange?logo=apachekafka)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Databricks-blue?logo=databricks)
![PySpark](https://img.shields.io/badge/PySpark-Streaming-brightgreen?logo=apache-spark)
![MLflow](https://img.shields.io/badge/MLflow-Tracking%2FInference-lightgrey?logo=mlflow)
![AWS](https://img.shields.io/badge/AWS-S3%20%2B%20CloudFront-yellow?logo=amazonaws)


## Project Website 
This project is fully documented and deployed as a static site using AWS S3, with CloudFront for global distribution and HTTPS, and Route 53 for custom domain routing.

🔹 Static Hosting via S3

🔹 CDN + HTTPS via CloudFront

🔹 Domain routing via Route 53

🔹 Includes: architecture diagrams, MLflow screenshots, notebook previews, and full pipeline breakdown

🌐 [**Explore the Full Project Website**](https://kafka-delta-pipeline.andreahayes-dev.com/)  

🎥 [**Watch the Video Walkthrough**](https://kafka-delta-pipeline.andreahayes-dev.com/video.html)


## Use Case Context

This pipeline simulates a clickstream analytics use case, where user behavior events — such as view, click, and purchase — are streamed in real-time from a web or mobile app.

It also applies anomaly detection using MLflow to identify unusual patterns in user interactions, such as excessive clicking or bot-like behavior.

This mirrors common use cases in e-commerce and ad tech platforms for detecting fraud, automation, or system misuse.

## End-to-End Flow

Confluent Kafka → PySpark Structured Streaming → Bronze Delta Table → Silver Layer → MLflow Inference → Gold Table

![Kafka → Delta Lake MLflow Pipeline Architecture](docs/mlflow_diagram.png)

**Diagram:** End-to-end architecture of the Kafka-to-Delta streaming pipeline with MLflow integration for batch inference.  

* * * * *

## Contents
- [Project Overview](#project-overview)
- [End-to-End Flow](#end-to-end-flow)
- [Architecture](#architecture)
- [Features Implemented (Sprint 1)](#features-implemented-sprint-1)
- [Workflow Orchestration (Databricks Workflows)](#workflow-orchestration-databricks-workflows)
- [Engineering Insights](#engineering-insights)
- [Performance & Observability](#performance-optimization-spark-ui)
- [Write Performance Benchmarking](#write-performance-benchmarking)
- [Spark Physical Plan](#spark-physical-plan)
- [ML Inference & Anomaly Detection](#ml-inference--anomaly-detection)
- [Inference Results & Output Layer](#inference-results--output-layer)
- [Cost Optimization](#cost--infra-optimization-sprint-4–5)
- [Project Structure](#project-structure)
- [Certification Alignment](#certification-alignment)

* * * * *

## Architecture

This pipeline simulates a real-time ETL system using:

🔹 Kafka (Confluent Cloud) for streaming ingestion

🔹 PySpark Structured Streaming for event processing

🔹 Delta Lake on Databricks for scalable, transactional storage

🔹 Databricks Workflows for orchestration and automation


## Features Implemented

🔹 Ingests synthetic events from Confluent Cloud Kafka

🔹 Parses binary Kafka payloads into structured JSON

🔹 Writes real-time data to a Bronze Delta table (append mode)

🔹 Uses checkpointing for replayability and fault tolerance

🔹 Secure Kafka auth using .env.template and Databricks secrets

🔹 Fully annotated PySpark notebooks for clarity and learning

* * * * *

## Workflow Orchestration (Databricks Workflows)

This pipeline is fully orchestrated using [Databricks Workflows](https://docs.databricks.com/workflows/index.html), 
automating the complete flow from Kafka ingestion to data quality monitoring.

Orchestrated Job Name: `full_streaming_pipeline`

It includes the following chained tasks:

1. `01_stream_kafka_to_bronze` – reads real-time events from Kafka and writes to a Delta Bronze table  
2. `02_bronze_to_silver_cleanse` – parses, deduplicates, and optimizes data into the Silver layer  
3. `03_monitor_silver_events` – monitors stream freshness and logs late events for observability

You can also explore the DAG config here: [`jobs/full_streaming_pipeline.json`](jobs/full_streaming_pipeline.json)


File path:

```bash
jobs/full_streaming_pipeline.json
``` 



## Workflow DAG (Databricks Workflows)

This streaming pipeline is orchestrated via Databricks Workflows.

Job: `full_streaming_pipeline`

**Orchestration DAG: Kafka to Delta pipeline via Databricks Workflows**
This DAG executes ingestion → transformation → monitoring with job task chaining.


![DAG Screenshot](docs/full_pipeline_dag.png)

* * * * *

## Engineering Insights

In addition to building the streaming pipeline, the following enhancements were made to simulate real-world production observability and performance tuning.



## Streaming Observability

This pipeline logs late/stale event metrics to a Delta table called `monitor_logs`.

You can inspect this table using:

```sql
SELECT * FROM monitor_logs ORDER BY run_time DESC
```

![Monitor Logs Preview](docs/monitor_logs_preview.png)

*Sample Output: `monitor_logs` Delta Table*


* * * * *

## Cluster Usage Tracking (Cost Control)

This pipeline logs manual cluster usage into a Delta table called `cluster_logs` for observability and cost awareness.

```sql
SELECT * FROM cluster_logs ORDER BY end_time DESC
```

![Cluster Logs Preview](docs/cluster_logs_preview.png)  
*Sample Output: cluster_logs Delta Table for task-level resource tracking*


* * * * *

## Performance Optimization (Spark UI)

This pipeline was benchmarked and optimized using the Spark UI and physical plans.

### Job-Level Execution Overview

![Spark Job Screenshot – Silver Write Overview](docs/spark_silver_write_jobs.png)  
*Spark UI job overview for the silver write benchmarking step.*

### Stage-Level Insight: Silver Write Task

![Spark Stage Detail – Task Execution](docs/spark_stage_silver_write.png)  
*Stage-level Spark UI view showing I/O and task distribution for the silver write operation.*

* * * * *

## Write Performance Benchmarking 

This project logs the runtime of key transformations (like Silver writes) to a Delta table called `benchmark_logs`.

```sql
SELECT * FROM benchmark_logs ORDER BY run_time DESC
```

![Benchmark Logs – Silver Write Duration](docs/benchmark_logs_preview.png)

*Sample Output: `benchmark_logs` table showing runtime duration for the `silver_write` task.*

* * * * *

## Spark Physical Plan

This project uses Spark’s physical plan inspection to understand and optimize the Silver layer transformation.

Output of `df_deduped.explain(mode="formatted")` before writing to `silver_events`:

![Spark Physical Plan – Silver Write](docs/physical_plan_silver_write.png)

* * * * *

## ML Inference & Anomaly Detection

This stage uses an Isolation Forest model (via `scikit-learn`) to detect anomalies in curated Silver Delta Lake events. Inference results are stored in the Gold layer and visualized.



### Pipeline Highlights

- Trained `IsolationForest` on features: `value`, `timestamp_unix`
- Logged model to MLflow (with input signature)
- Tracked parameters (`contamination`, `features`) and training record count
- Registered model in MLflow Model Registry
- Scored over 1,000 Silver events and generated:
  - `anomaly_score`: continuous score
  - `anomaly_flag`: binary (`-1` = anomaly, `1` = normal)
- Wrote results to `gold_anomaly_predictions` and `gold_events_scored` Delta tables

* * * * *

### MLflow Model Logging

The trained model was logged to MLflow with:

- Input signature via `input_example`
- Parameters: contamination rate, feature names
- Metric: number of training records
- Registered in MLflow for versioned lifecycle management

**Model Name:** `iforest_silver_anomaly_detector`  
**Version:** 1  
**Registry Scope:** Workspace  
**Owner:** Andrea Hayes

![MLflow Model Logging](docs/mlflow_model_logging.png)

---

### 🧾 MLflow Model Registry

This project uses the **MLflow Model Registry** to manage lifecycle stages of trained models.

The Isolation Forest model (`iforest_silver_anomaly_detector`) is logged and registered with:

- **Version control** (e.g. Version 1)
- **Owner metadata**
- **Workspace-level visibility**

This registry entry enables reproducible scoring, auditing, and deployment to other environments.

![MLflow Model Registry](docs/mlflow_model_registry.png)  

*Sample Output: MLflow registry UI showing the registered Isolation Forest model and its version metadata.*

---

### 📈 MLflow Tracking – Batch Inference

This MLflow run captures metrics, parameters, and artifacts logged during the Silver → Gold scoring pipeline. It tracks the input schema, average anomaly score, and links directly to the registered model.

![MLflow run overview](docs/mlflow_run_overview.png)  

*Sample Output: MLflow run UI showing metadata, parameters, and source notebook used for inference.*


---

### 📊 MLflow Tracked Metrics

MLflow metrics for the batch inference run (`Batch Inference: Silver to Gold`):

- **1020 events scored**
- **Average anomaly score: 0.80**

These metrics are automatically logged and versioned for every pipeline run, enabling reproducibility, performance tracking, and model monitoring.

![MLflow Tracked Metrics](docs/mlflow_metrics_visualized.png)

---

### Sample Inference Output

This preview shows scored user interaction events from the Silver table. Anomaly scores are generated via an Isolation Forest model logged and deployed with MLflow.

![Sample Scored Events](docs/scored_events_preview.png)

* * * * *

### Inference Results & Output Layer



### Gold Delta Table

![Gold Delta Table](docs/gold_delta_predictions.png)  
**Sample Output:** A preview of the `gold_anomaly_predictions` Delta table with anomaly scores and flags.

* * * * *

### Top Anomaly Query

```sql
SELECT * 
FROM gold_anomaly_predictions 
WHERE anomaly_flag = -1 
ORDER BY anomaly_score ASC 
LIMIT 20
```

![Top Anomalies Query](docs/top_anomalies_query.png)  

*Sample Output: Top 20 most anomalous events scored by the Isolation Forest model.*

* * * * *

### Anomaly Score Distribution

![Anomaly Score Histogram](docs/anomaly_score_hist.png)  

*Sample Output: Distribution of Isolation Forest anomaly scores — left tail indicates flagged outliers.*


* * * * *

### Gold Table Output – Scored Events

![Gold Table Output](docs/gold_table_preview.png)

Final output written to the `gold_events_scored` Delta table using Structured Streaming.

Includes event ID, type, timestamp, and original value — enriched with MLflow-inferred anomaly scores.

Partitioned by `event_type` and includes metadata such as `inference_ts` and `mlflow_run_id` (not shown).

* * * * *

### Anomaly Detection Visuals

Confusion matrix and anomaly score distribution from Isolation Forest predictions.  
Threshold-based labeling enables binary classification from unsupervised scores.

![Anomaly Detection Visuals](docs/anomaly_eval_visuals.png)

* * * * *

### Delta Gold Table + Audit History

The ML inference results are written to a partitioned Delta table called `gold_events_scored`.  
This screenshot highlights how **Delta Lake tracks full lineage and version history** using:

- `anomaly_score` output  
- Inference timestamp  
- Associated `mlflow_run_id` for reproducibility

The `DESCRIBE HISTORY` command provides **auditable tracking** of every write, including the operation, user, and schema evolution.

![Gold Delta Output + History](docs/gold_write_and_history.png)

* * * * *

## Project Structure

<pre> 
kafka-delta-streaming-pipeline/
├── bronze/
│   └── 01_stream_kafka_to_bronze.py
├── silver/
│   ├── 02_bronze_to_silver_cleanse.py
│   └── 03_monitor_silver_events.py
├── gold/
│   ├── 05_batch_inference_to_gold.py
│   └── 06_mlflow_inference_logging.py
├── utils/
│   ├── kafka_schema.py
│   └── log_cluster_activity.py
├── config/
│   └── .env.template
├── data/
│   └── synthetic_data_sample.json
├── jobs/
│   └── full_streaming_pipeline.json
├── docs/
│   ├── README_notes.md
│   └── [Screenshots + Diagrams]
├── .gitignore
└── README.md


 </pre>

 * * * * *

## Sample Kafka Event

{
  "event_id": "997",
  "event_type": "click",
  "timestamp": "2025-04-01T17:37:57.000Z",
  "value": "1984"
}

* * * * *

Roadmap
----------

### **Pipeline Development**

-   **Kafka cluster + producer simulation**\
    Set up a Kafka cluster for event streaming and simulate event production.

-   **Bronze Layer Ingestion**\
    Implemented real-time ingestion of raw Kafka events into the Delta Bronze table.

-   **Silver Layer Transformation**\
    Applied transformations to the data (filtering, deduplication) for improved quality and consistency.

-   **Delta Optimization**\
    Optimized Delta tables using partitioning and ZORDER for better query performance.

### **Advanced Features & ML Integration**

-   **MLflow Model Training & Tracking**\
    Trained and tracked the Isolation Forest model for anomaly detection using MLflow.

-   **Batch Inference to Gold Layer**\
    Performed batch scoring on transformed data and wrote inference results to the Gold Delta table.

-   **Spark Performance Tuning**\
    Focused on Spark optimizations for performance and cost control.

-   **Final Documentation & Demo Video**\
    Cleaned up the repo, created detailed documentation, and recorded a demo video.

* * * * *

⚡ Cost & Infrastructure Optimization
------------------------------------

This project includes several cloud-scale optimization techniques for cost control and performance improvement:

-   **Spot Instances + Auto-Termination** on Databricks clusters to minimize costs during idle times.

-   **Cluster Usage Logging** to Delta (`cluster_logs`) for cost awareness and analysis.

-   **Lightweight Benchmarking** to track performance and optimize transformation times (`benchmark_logs`).

-   **Partitioning & ZORDER** for optimized queries and reduced storage cost.

These practices simulate a real-world production environment for cost efficiency and scalability.

* * * * *

## Certification Alignment

This project is built to align with the Databricks Certified Data Engineer Professional exam covering:
* Structured Streaming
* Delta Lake intricacies
* Production-level best practices
* Advanced MLflow integration