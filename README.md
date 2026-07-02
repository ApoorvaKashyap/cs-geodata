# cs-geodata

A converter for CoREStack data to cloud-native forms.

## Overview

This project provides a FastAPI-based REST API and a background worker system (using RQ and Redis) to convert geographical data into cloud-native formats. It leverages high-performance data processing libraries like DuckDB, Polars, and GeoPandas.

## Architecture

* **App**: A FastAPI application that serves the REST endpoints.
* **Workers**: Background worker processes that handle the heavy lifting of data conversion.
* **Redis**: Used as a message broker for the work queue.

## Getting Started

### Prerequisites

* Docker
* Docker Compose

### Running Locally

You can spin up the entire application stack using Docker Compose:

```bash
docker compose up -d --build
```

This will start the following services:
* `redis`: The Redis message broker.
* `workers`: The background worker processes.
* `app`: The FastAPI application, accessible at http://localhost:8000.

### Development

The `compose.yaml` is configured with `watch` enabled for active development.
* Changes to Python files will automatically sync and restart the application.
* Changes to `pyproject.toml` will rebuild the application.

If you are running outside of Docker for development, you can use `uv` (as the project uses `uv.lock`) or `pip` to install dependencies from `pyproject.toml`.

## API Endpoints

* `GET /`: Health check to verify Redis connection and worker status.
* `GET /api/v1/status?task_id={id}`: Get the status of a specific background job.
* `POST /api/v1/vector/layers`: Submit a request to convert vector layers.
* `POST /api/v1/vector/create_base_cache`: Submit a request to create a base cache for layers.

## Tech Stack

* **Web Framework**: FastAPI, Uvicorn
* **Data Processing**: DuckDB, Polars, GeoPandas
* **Task Queue**: RQ, Redis
* **Cloud Storage**: Boto3, S3FS
