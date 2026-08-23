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


