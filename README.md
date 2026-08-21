# Wikipedia Real-Time Streaming & Historical Batch Processing Architecture

A dual-mode data engineering solution deployed on **Microsoft Azure**. This architecture captures **live Wikimedia edit events** in real time for immediate alerting while simultaneously ingesting **historical pageview metrics** via scheduled batch pipelines into an Azure Data Lake Storage Gen2 bronze layer.

---

## 1. System Architecture Diagram

+-----------------------------------------------------------------------+
|                    Wikimedia SSE Event Stream                         |
|                 (stream.wikimedia.org/v1/events)                      |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|               Azure App Service (Flask App)                           |
|               Python Producer & Batch Buffer                          |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|                    Azure Event Hubs                                   |
|                (wikipedia-live-stream)                                |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|                   Azure Logic Apps                                    |
|              Parse JSON & Filter (bot, enwiki)                        |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|                  Email / Teams Alerts                                 |
+-----------------------------------------------------------------------+

=========================================================================

+-----------------------------------------------------------------------+
|                    Wikimedia REST API                                 |
|              (api/rest_v1/metrics/pageviews)                          |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|                Azure Data Factory (ADF)                               |
|              ForEach Pipeline Dynamic Ingestion                       |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------+-----------------------------------+
|           Azure Data Lake Storage Gen2 (ADLS)                         |
|                 (Bronze Layer Container)                              |
+-----------------------------------------------------------------------+

2. Pipeline Architecture Overview
Real-Time Streaming Pipeline
 * Ingestion: Custom Python Flask producer running on Azure App Service connecting to Wikimedia's SSE endpoint (stream.wikimedia.org).
 * Buffering & Messaging: Micro-batches of 20 events published directly to Azure Event Hubs (wikipedia-live-stream).
 * Alerting Engine: Azure Logic App triggered on event availability, decoding Base64 payload, parsing properties (bot, type, wiki), filtering for human edits on English Wikipedia, and routing real-time notifications.
Historical Batch Ingestion Pipeline
 * Orchestration: Azure Data Factory (wikipediaadf) executing parameterized dynamic HTTP REST API requests.
 * Iteration Logic: ForEach activity iterating through target month parameters (MonthsArray = ["2026/06", "2026/07"]).
 * Storage Sink: Dynamic JSON file storage into Azure Data Lake Storage Gen2 (adl_wikipedia) inside the bronze container with hierarchy flattening.
3. Tech Stack & Services Used
 * Cloud Provider: Microsoft Azure
 * Compute & Web Hosting: Azure App Service (Gunicorn / Flask)
 * Real-Time Streaming: Azure Event Hubs
 * Workflow Automation & Alerting: Azure Logic Apps
 * Batch ETL Orchestration: Azure Data Factory (ADF)
 * Data Storage: Azure Data Lake Storage Gen2 (ADLS Gen2)
 * Programming Languages: Python 3.x, JSON, Azure Expression Language
4. Azure Data Factory Detailed Setup
Linked Services
 * adl_wikipedia (Azure Data Lake Storage Gen2): Connected via Storage Account Key pointing to [https://flightstacc.dfs.core.windows.net/](https://flightstacc.dfs.core.windows.net/).
 * Httpwikipediaurl (HTTP): Base URL configured to [https://wikimedia.org/](https://wikimedia.org/) with Anonymous authentication.
Datasets
 * datasethttps (HTTP Source JSON):
   * Relative URL Expression: @concat('api/rest_v1/metrics/pageviews/aggregate/all-projects/all-access/user/monthly/', dataset().MonthPath, '/0100/2026080100')
 * sinkadlwikipedia (ADLS Gen2 Sink JSON):
   * Linked Service: adl_wikipedia
   * File Path: Container bronze, Directory @concat('wikipedia/batch_historical/', dataset().MonthPath), File Name pageviews_data.json
Pipeline Flow (pl-wikipedia_batch_ingest)
 * Pipeline Parameter: MonthsArray (Array type) populated with target processing dates.
 * ForEach1 Activity: Sequential/parallel iteration over @pipeline().parameters.MonthsArray.
 * Copy data1 Activity:
   * Source: datasethttps passing dynamic parameter MonthPath = @item()
   * Sink: sinkadlwikipedia passing parameter MonthPath = @item() with Flatten hierarchy enabled.
5. Storage Folder Hierarchy (ADLS Gen2)
adl_wikipedia (ADLS Gen2 Container: bronze)
└── wikipedia/
    └── batch_historical/
        ├── 2026/06/
        │   └── pageviews_data.json
        └── 2026/07/
            └── pageviews_data.json

6. Real-Time Logic App Filter Rules
The Azure Logic App evaluates incoming Event Hub message bodies using the following rules:
 * JSON Parsing: Decodes Base64 message stream into valid JSON object.
 * Condition Criteria:
   * bot == false (Excludes automated bot edits)
   * type == "edit" or type == "new" (Filters structural change events)
   * wiki == "enwiki" (Isolates English Wikipedia domain traffic)
7. Operational Monitoring & Validation
 * Event Hub Metrics: Confirmed active ingress/egress spikes and payload throughput across $Default consumer group.
 * App Service Log Stream: Verified steady generation and push of 20-event micro-batches.
 * ADF Execution Runs: Debug pipeline runs confirmed successful ingestion of dynamic monthly pageview datasets, completing each batch run within 27 to 36 seconds.
