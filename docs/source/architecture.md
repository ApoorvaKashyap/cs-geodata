# Architecture

`cs-geodata` is designed as an asynchronous, distributed system to handle geographical data processing.

## Components

### 1. Web Application (FastAPI)
The entry point for the service. It handles REST requests and offloads heavy data-processing tasks to background workers.

### 2. Task Queue (RQ & Redis)
* **Redis** acts as the message broker, storing tasks that need to be processed.
* **RQ (Redis Queue)** manages the queues and task execution.
* Four named queues are used: `layers` (conversion jobs), `base` (base layer cache jobs), `meta` (per-tehsil download jobs), and `id` (identity jobs).

### 3. Workers
Dockerized background processes that pick up jobs from Redis. These workers utilize high-performance libraries like **DuckDB**, **Polars**, and **GeoPandas** to process the geospatial data efficiently and export it to cloud-native formats.

### 4. Cloud Storage
Processed files (like parquet or geojson) can interact with cloud storage using **Boto3** and **S3FS**.

## System Diagram

```{mermaid}
graph TB
    Client(["Client"])

    subgraph API ["FastAPI (app)"]
        EP_layers["POST /api/v1/vector/layers"]
        EP_base["POST /api/v1/vector/create_base_cache"]
        EP_status["GET /api/v1/status"]
    end

    subgraph Redis ["Redis"]
        Q_layers[("layers queue")]
        Q_base[("base queue")]
        Q_meta[("meta queue")]
    end

    subgraph Workers ["Workers (Docker)"]
        W_layers["Layer Worker<br/>run_pipeline()"]
        W_base["Base Worker<br/>convert_base()"]
        W_meta["Meta Workers<br/>download_and_convert_geojson()<br/>× N tehsils"]
    end

    Storage[("S3 / Local Storage<br/>Descriptors · Base layers<br/>Temp Parquets · Output")]
    GeoServer["GeoServer<br/>WFS Endpoints"]

    Client -->|"POST descriptor_url + output_path"| EP_layers
    Client -->|"POST base_layer_source"| EP_base
    Client -->|"GET task_id"| EP_status

    EP_layers -->|"enqueue job"| Q_layers
    EP_base -->|"enqueue job"| Q_base
    EP_status -->|"fetch job status"| Q_layers

    Q_layers --> W_layers
    Q_base --> W_base

    W_layers -->|"fetch TOML descriptor"| Storage
    W_layers -->|"read base Parquet"| Storage
    W_layers -->|"enqueue 1 job per tehsil"| Q_meta
    Q_meta --> W_meta
    W_meta -->|"WFS GeoJSON fetch"| GeoServer
    W_meta -->|"write temp Parquet"| Storage
    W_layers -->|"scan temp Parquets"| Storage
    W_layers -->|"write output"| Storage

    W_base -->|"read source file"| Storage
    W_base -->|"write base Parquet"| Storage
```
