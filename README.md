# Wikipedia Real-Time Streaming & Historical Batch Processing Architecture

A dual-mode data engineering solution deployed on **Microsoft Azure**. This architecture captures **live Wikimedia edit events** in real time for immediate alerting while simultaneously ingesting **historical pageview metrics** via scheduled batch pipelines into an Azure Data Lake Storage Gen2 bronze layer.

---

## 1. System Architecture Overview

### Real-Time Streaming Pipeline

| Step | Service / Component | Config Details | Core Function |
| :--- | :--- | :--- | :--- |
| **1. Source** | **Wikimedia SSE Stream** | `stream.wikimedia.org/v1/events` | Continuous live event publishing for Wikipedia edits. |
| **2. Producer** | **Azure App Service (Flask)** | Python Application | Consumes SSE events & creates 20-item micro-batches. |
| **3. Ingestion**| **Azure Event Hubs** | Hub: `wikipedia-live-stream` | High-throughput stream ingestion queue. |
| **4. Processing**| **Azure Logic Apps** | Consumer Group: `$Default` | Parses Base64 JSON & applies filters (`bot`, `wiki`). |
| **5. Action** | **Alerting Engine** | Email / Teams Alerts | Triggers immediate real-time notifications. |

### Historical Batch Ingestion Pipeline

| Step | Service / Component | Config Details | Core Function |
| :--- | :--- | :--- | :--- |
| **1. Source** | **Wikimedia REST API** | `api/rest_v1/metrics/pageviews` | Provides aggregate monthly article pageview data. |
| **2. Pipeline** | **Azure Data Factory (ADF)** | Pipeline: `pl-wikipedia_batch_ingest` | Dynamic ingestion driven by parameterized `ForEach` loop. |
| **3. Target Sink**| **Azure Data Lake Storage Gen2** | Container: `bronze` | Persists historical JSON files in raw landing folder. |

---

## 2. Pipeline Architecture Details

### Real-Time Streaming Pipeline
* **Ingestion:** Custom Python Flask producer running on Azure App Service connecting to Wikimedia's SSE endpoint (`stream.wikimedia.org`).
* **Buffering & Messaging:** Micro-batches of 20 events published directly to Azure Event Hubs (`wikipedia-live-stream`).
* **Alerting Engine:** Azure Logic App triggered on event availability, decoding Base64 payload, parsing properties (`bot`, `type`, `wiki`), filtering for human edits on English Wikipedia, and routing real-time notifications.

### Historical Batch Ingestion Pipeline
* **Orchestration:** Azure Data Factory (`wikipediaadf`) executing parameterized dynamic HTTP REST API requests.
* **Iteration Logic:** `ForEach` activity iterating through target month parameters (`MonthsArray = ["2026/06", "2026/07"]`).
* **Storage Sink:** Dynamic JSON file storage into Azure Data Lake Storage Gen2 (`adl_wikipedia`) inside the `bronze` container with hierarchy flattening.

---

## 3. Tech Stack & Services Used

* **Cloud Provider:** Microsoft Azure
* **Compute & Web Hosting:** Azure App Service (Gunicorn / Flask)
* **Real-Time Streaming:** Azure Event Hubs
* **Workflow Automation & Alerting:** Azure Logic Apps
* **Batch ETL Orchestration:** Azure Data Factory (ADF)
* **Data Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Programming Languages:** Python 3.x, JSON, Azure Expression Language

---

## 4. Azure Data Factory Detailed Setup

### Linked Services
* **`adl_wikipedia`** (Azure Data Lake Storage Gen2): Connected via Storage Account Key pointing to `https://flightstacc.dfs.core.windows.net/`.
* **`Httpwikipediaurl`** (HTTP): Base URL configured to `https://wikimedia.org/` with Anonymous authentication.

### Datasets
* **`datasethttps`** (HTTP Source JSON):
  * **Relative URL Expression:** `@concat('api/rest_v1/metrics/pageviews/aggregate/all-projects/all-access/user/monthly/', dataset().MonthPath, '/0100/2026080100')`
* **`sinkadlwikipedia`** (ADLS Gen2 Sink JSON):
  * **Linked Service:** `adl_wikipedia`
  * **File Path:** Container `bronze`, Directory `@concat('wikipedia/batch_historical/', dataset().MonthPath)`, File Name `pageviews_data.json`

### Pipeline Flow (`pl-wikipedia_batch_ingest`)
1. **Pipeline Parameter:** `MonthsArray` (Array type) populated with target processing dates.
2. **`ForEach1` Activity:** Sequential/parallel iteration over `@pipeline().parameters.MonthsArray`.
3. **`Copy data1` Activity:** 
   * **Source:** `datasethttps` passing dynamic parameter `MonthPath = @item()`
   * **Sink:** `sinkadlwikipedia` passing parameter `MonthPath = @item()` with `Flatten hierarchy` enabled.

---

## 5. Storage Folder Hierarchy (ADLS Gen2)

```text
adl_wikipedia (ADLS Gen2 Container: bronze)
└── wikipedia/
    └── batch_historical/
        ├── 2026/06/
        │   └── pageviews_data.json
        └── 2026/07/
            └── pageviews_data.json.

```

# VIDEO: 



https://github.com/user-attachments/assets/a9c937c4-2219-4611-8065-79e056959f8f



## 🔄 Azure Databricks Data Ingestion & Processing Architecture

This phase handles the secure ingestion, structural transformation, and optimized storage of both real-time streaming edits and historical pageview batch data using **Azure Databricks** and **PySpark**.

---

### 🛡️ Enterprise Security & Secrets Management
* **Why it matters:** Hardcoding sensitive cloud connection keys directly in notebooks poses a severe security risk and violates enterprise compliance standards.
* **How it works:** Access keys for Azure Data Lake Storage (ADLS Gen2) are stored within an encrypted Databricks Secret Scope (`dbutils.secrets.get(scope="wikimedia", key="Access-key")`). This guarantees that authentication tokens remain completely hidden from logs, visual outputs, and version control while maintaining seamless cloud authorization.

---

### 🌊 Real-Time Edit Stream Ingestion (`Stream_processing_pyspark`)
* **Why it matters:** Wikimedia's live edit feeds emit continuously as raw binary Avro/JSON event streams containing nested metadata. This raw format must be sanitized and flattened before analysts or analytical engines can use it.
* **How it works:**
  * **Data Lake Reading:** Databricks recursively reads raw event logs stored in binary format directly from the ADLS Gen2 `bronze` container.
  * **Schema Extraction & Decoding:** Converts raw binary payloads into human-readable strings, dynamically infers the nested JSON structure, and unpacks the embedded change details.
  * **Field Normalization:** Filters out noise and extracts critical attributes—including `sequence_number`, `enqueued_time`, `id`, `timestamp`, `title`, `bot`, `user`, `type`, and `wiki`.
  * **Parquet Optimization:** Writes the clean dataset as partitioned **Parquet** files into the staging area. Parquet provides columnar compression, drastically reducing storage costs and accelerating query execution in Snowflake.

---

### 📦 Historical Pageview Batch Ingestion (`Batch_processing_pyspark`)
* **Why it matters:** Wikimedia pageviews are delivered as massive, deeply nested JSON objects containing multi-layered arrays (projects, monthly view totals, daily details, and top-ranked articles). SQL warehouses struggle with nested arrays, leading to slow queries and complex joins.
* **How it works:**
  * **Multi-Line JSON Ingestion:** Reads large multi-line JSON files representing historical pageview metrics from the data lake.
  * **Array Unnesting (Explode):** Utilizes PySpark's array explosion functions (`explode_outer` and `explode`) to flatten nested lists into individual database rows without losing parent-child relationships.
  * **Attribute Extraction:** Unpacks structured fields into clear relational columns: `project`, `access`, `year`, `month`, `day`, `article`, `views`, and `rank`.
  * **Storage Delivery:** Saves the fully flattened, schema-aligned dataset into Parquet format in the ADLS Gen2 staging zone for direct downstream consumption.

---

### ⏱️ Automated Pipeline Scheduling & Reliability
* **Why it matters:** Data pipelines require consistent automated execution to keep downstream reports fresh while managing cloud compute costs efficiently.
* **How it works:**
  * **Databricks Workflows:** The transformation tasks are orchestrated through a dedicated Databricks Job pipeline (`Stream_processing_pyspark`).
  * **12-Hour Schedule:** Configured to run automatically every 12 hours. This cadence balances real-time metric updates with optimal cluster resource consumption, preventing idle compute billing.
  * **Run Monitoring & Alerts:** Monitors total job duration (averaging ~6 minutes per run) and captures execution metrics to ensure continuous health and immediate failure visibility.


#  VIDEO Pyspark:



https://github.com/user-attachments/assets/1ad46aa3-0bfe-4c9e-b325-efd7c141c10a


## 🚀 Automated CI/CD Deployment Pipeline (`deploy_pyspark.yml`)

This project implements an enterprise-grade Continuous Integration and Continuous Deployment (CI/CD) pipeline using **GitHub Actions** and the **Databricks CLI**. It automates code validation, dependency installation, testing, and workspace synchronization to guarantee reliable, zero-downtime deployments.

---

### 🛠️ Stage 1: Continuous Integration (Linting & Unit Testing)
* **Why it matters:** Manually pushing code directly to production workspace folders can introduce syntax errors, broken dependencies, or untested PySpark logic. Automated CI acts as a quality gate, verifying every pull request and push to `main`.
* **How it works:**
  * **Event Triggers:** Automatically fires on every `push` or `pull_request` targeting the `main` branch.
  * **Environment Provisioning:** Spins up an isolated `ubuntu-latest` runner equipped with Python 3.10.
  * **Dependency Installation:** Automatically upgrades `pip` and installs essential pipeline tools including `pytest`, `pyspark`, and `databricks-cli`.
  * **Automated Testing Gate:** Runs unit tests across PySpark modules to validate schema definitions and transformation logic before allowing code to advance to deployment.

---

### 🚀 Stage 2: Continuous Deployment (Deploy to Databricks)
* **Why it matters:** Automating code sync to Databricks eliminates manual file uploads, prevents human error, and ensures the production cluster always runs the exact commit present in the primary Git repository.
* **How it works:**
  * **Conditional Execution:** Executes strictly after Stage 1 completes successfully and only when code is merged directly into `refs/heads/main`.
  * **Databricks CLI Setup:** Initializes the official `databricks/setup-cli@main` action on the runner.
  * **Secure Authentication:** Connects to the target workspace using repository secrets (`DATABRICKS_HOST` and `DATABRICKS_TOKEN`), keeping credentials encrypted and completely hidden from build logs.
  * **Workspace Synchronization:** Executes `databricks workspace import-dir . /Shared/pyspark_pipeline --overwrite` to push all validated PySpark scripts directly into the shared Databricks workspace folder, updating downstream workflow jobs in real time.
 
 # CI/CD Video:

 

https://github.com/user-attachments/assets/c66e4a42-6968-4f75-88a2-3d1e8a867507

## ❄️ Snowflake Data Lakehouse Architecture & Ingestion Strategy

### 💡 Architectural Decisions: WHY Snowflake & External Stages?
To process Wikimedia's batch metric logs alongside real-time streaming data, Snowflake serves as the central Enterprise Data Warehouse. Instead of directly ingesting unorganized data into managed internal storage—which inflates storage overhead—the pipeline leverages **Storage Integrations** and **External Stages** connected directly to Azure Data Lake Storage Gen2 (ADLS Gen2) Bronze containers.

* **Separation of Compute & Storage:** Raw Parquet files remain stored cost-effectively inside ADLS Gen2. Snowflake compute (`COMPUTE_WH`) is strictly isolated and only spins up when executing transformations or running automated load schedules.
* **Schema-on-Read Optimization:** Incoming raw files are structured in binary Parquet format containing nested JSON/Variant payloads. Using Schema-on-Read pathing (`$1:column_name::DATATYPE`), raw payloads are dynamically extracted and explicitly cast into strongly typed SQL columns without needing intermediate staging tables.

---

### 🛠️ Core Infrastructure Breakdown & Explanations

#### 1. Cloud-Native Storage Integration (`azure_adls_snowflake_int`)
* **Why it's configured:** Establishes a permanent, secure IAM trust relationship between Azure Tenant and Snowflake. 
* **Engineering Impact:** Restricts Snowflake's access scope strictly to authorized Blob container paths (`azure://flightstacc.blob.core.windows.net/bronze/`), preventing unauthorized data surface access.

#### 2. External Stages (`adls_batch_stage` & `adls_api_stage`)
* **Why it's configured:** Creates pointer references inside Snowflake targeting exact Azure folder paths for both batch and stream datasets using a centralized `PARQUET` file format.
* **Engineering Impact:** Standardizes data parsing rules across heterogeneous input channels (Batch analytics vs. Streaming event metrics) while keeping storage references modular.

#### 3. Schema-on-Read Ingestion & Column Casting
* **Why it's configured:** Direct parsing of `$1` (Variant object) allows extraction of explicit fields such as `project`, `views`, `rank`, and temporal metrics (`year`, `month`, `day`) directly from staged files.
* **Engineering Impact:** Eliminates heavy pre-processing scripts prior to data lake ingestion, reducing pipeline runtime and latency.

#### 4. Automated Task Scheduling (`wikimedia_stream_task`)
* **Why it's configured:** A native Snowflake Task executes a scheduled `COPY INTO` query every 720 minutes to incrementally append incoming micro-batch/streaming files into production tables.
* **Engineering Impact:** Automates the ingestion lifecycle without requiring external orchestrators for simple polling, maintaining high data freshness with zero manual intervention.

# video_snowflake:

https://github.com/user-attachments/assets/7a9ab0dd-ac45-403e-89d5-1ef3b2835245


# 🚀 Wikimedia Data Engineering Pipeline DBT with Snowflake 

A production-grade Analytics Engineering pipeline transforming raw Wikimedia streaming and batch data into enterprise-grade dimensional models using **dbt**, **Snowflake**, and **GitHub Actions CI/CD**.

---

## 🏗️ Architecture & Processing Design

This pipeline follows the **Kappa Architecture** & **Medallion  Architecture** design pattern to seamlessly unify high-velocity real-time event streams with historical batch data into a single queryable analytics engine.

```text
  [ Real-Time Stream: Wikimedia SSE ] ──► Kafka / Event Hubs ──► Snowflake Stream Table ┐
                                                                                         ├──► [ UNION ALL Strategy ] ──► Intermediate Engine ──► Analytics Marts
  [ Batch Data: Daily Pageviews ] ─────► ADLS / Databricks ────► Snowflake Batch Table  ┘
```
💡 Why Hybrid UNION ALL Strategy?
 * Unified Analytics Engine: Eliminates siloed data storage by standardizing real-time edit telemetry and historical page view counts into a single source of truth (int_wikimedia_events).
 * High-Performance Deduplication: Combines stream and batch events using standard surrogate keys (md5(concat(...))) and applies ROW_NUMBER() OVER (PARTITION BY surrogate_key ORDER BY event_timestamp DESC) to guarantee exact-once semantics without heavy database locks.
 * Optimized Cost Efficiency: Prevents expensive full-table scans by filtering and materializing streaming/batch splits upfront before dimensional aggregation.


```text

📁 Repository Structure
Wikimedia-DataEngineering-Pipeline/
├── .github/
│   └── workflows/
│       ├── dbt_ci_cd.yml          # GitHub Actions CI/CD Pipeline
│       └── deploy_pyspark.yml
├── models/
│   └── wikipedia/
│       ├── INTERMEDIATES/
│       │   └── int_wikimedia_events.sql
│       ├── marts/
│       │   ├── dim_editors.sql
│       │   ├── fct_artical_daily_metrics.sql
│       │   └── fct_hourly_traffic_spikes.sql
│       ├── TEST/
│       │   └── schema.yml         # Data quality tests & column descriptions
│       ├── source.yml
│       ├── stg_wikimedia_batch.sql
│       └── stg_wikimedia_streaming.sql
├── profiles.yml
├── dbt_project.yml
└── target/
```

```text

🔗 Data Lineage & Directed Acyclic Graph (DAG)
[ raw_source.WIKIMEDIA_STREAM_DATA ] ──► [ stg_wikimedia_streaming ] ──┐
                                                                      ├──► [ int_wikimedia_events ] ──┬──► [ dim_editors ]
[ raw_source.WIKIMEDIA_BATCH_DATA  ] ──► [ stg_wikimedia_batch     ] ──┘                              ├──► [ fct_artical_daily_metrics ] (BI Dashboard)
                                                                                                      └──► [ fct_hourly_traffic_spikes ] (Streamlit App)
```

🛠️ Data Transformation Layers & Marts
1. Staging & Intermediate Layer (stg_, int_)
 * stg_wikimedia_streaming & stg_wikimedia_batch: Cleans raw JSON payloads, casts timestamp fields (to_timestamp), handles missing values, and standardizes column mappings.
 * int_wikimedia_events: Merges batch and streaming CTEs using UNION ALL and applies surrogate key deduplication to maintain unique records.
2. Analytics Marts (marts/)
 * dim_editors: Dimension table categorizing editor metrics (identifying bots vs. human editors, total contributions, and active timestamps).
 * fct_hourly_traffic_spikes (Streamlit App Integration): High-frequency hourly aggregations capturing live traffic fluctuations, rapid edit spikes, and active contributor counts in real time.
 * fct_artical_daily_metrics (BI Dashboards Integration): Aggregated daily key metrics including total edits, unique editors, bot vs. human edit ratios, and cumulative article pageviews.
⚙️ Automated CI/CD & dbt Orchestration
Integrated with GitHub Actions for Automated Testing and Continuous Delivery (CI/CD), as well as dbt Cloud Orchestration for scheduled execution.
GitHub Actions Pipeline (.github/workflows/dbt_ci_cd.yml)
Automated verification triggers on every Pull Request or push to main:
 * Environment Provisioning: Dynamically sets up Python 3.10 and installs dbt-snowflake.
 * Profile Generation: Constructs profiles.yml dynamically utilizing encrypted GitHub Secrets (DBT_ENV_SECRET_*).
 * Automated Verification: Runs dbt deps, dbt debug, and executes models/tests via dbt build.
🛡️ Data Quality Testing & Monitoring
dbt Schema Assertions (schema.yml)
 * unique & not_null: Enforced across primary identifiers like username.
 * accepted_values: Categorical validation for boolean flags (is_bot: [true, false]).
 * Non-Blocking Alert Thresholds: Configured with severity: warn parameters to log alerts during CI test runs without blocking streaming ingestion.
Snowflake Horizon Catalog Integration
 * Accuracy & Uniqueness: Monitors NULL COUNT and tracks DUPLICATE COUNT metrics on surrogate identifiers (EVENT_ID, SEQUENCE_NUMBER).
 * Volume & Freshness Monitoring: Automated row count trends and execution monitoring within physical target schemas (WIKIMEDIA_DB.PUBLIC).
🔧 Troubleshooting: Snowflake Warehouse Execution Errors

# DBT PROCESS VIDEO:

https://github.com/user-attachments/assets/1390c5ff-58a9-40b8-984e-2be3a5a54b92



# DBT DEPLOY CI/CD:

https://github.com/user-attachments/assets/2bf4e020-146f-482e-9912-252aae99b2f6















