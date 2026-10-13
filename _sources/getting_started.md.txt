# Getting Started

This guide explains how to get the `cs-geodata` service running locally.

## Prerequisites

* Docker
* Docker Compose

## Running Locally

You can spin up the entire application stack using Docker Compose:

```bash
docker compose up -d --build
```

This will start the following services:
* `redis`: The Redis message broker (used for task queues).
* `workers`: Background worker processes.
* `app`: The FastAPI application.

## Development Setup

The `compose.yaml` is configured with `watch` enabled for active development.
* Changes to Python files will automatically sync and restart the application.
* Changes to `pyproject.toml` will rebuild the application.

If you are running outside of Docker for development, install dependencies using `uv`:

```bash
uv sync
```
or with `pip`:
```bash
pip install -e ".[dev,docs]"
```

## Running the documentation locally

To build the documentation, navigate to the `docs` folder:

```bash
cd docs
make html
```

The generated files will be available in `docs/build/html/index.html`.
