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
            └── pageviews_data.json
