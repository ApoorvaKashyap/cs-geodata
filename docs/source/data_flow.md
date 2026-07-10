# Data Flow

This document traces a complete pipeline run — from the initial API call through to the final Parquet files on disk or S3.

## Step 1 — API receives the request

The client sends a `POST /api/v1/vector/layers` request with two fields:

- `descriptor_url` — S3 URI, HTTPS URL, or GitHub raw URL pointing to the TOML descriptor.
- `output_path` — destination directory for the output Parquet files (local or `s3://`).

The `ConversionRequest` model validates the payload. The API enqueues a **lightweight job** (just the URL + path) onto the `layers` Redis queue and immediately returns:

```json
{ "task_id": "abc123", "status": "queued" }
```

The client can poll `GET /api/v1/status?task_id=abc123` at any time.

---

## Step 2 — Worker loads and validates the descriptor

A background worker (running in a separate Docker container) dequeues the job and calls `layer_conversion`. It fetches the TOML from the URL and parses it into a `LayerConversionRequest`, validated by Pydantic:

- `base` must match exactly one `[[layers]]` entry by `name`.
- `super_layer_source` and `super_field` must both be set or both absent.

If validation fails the job is marked **failed** and the error is logged.

---

## Step 3 — Tehsil version filtering

If `active_locations` is provided, the worker fetches the version CSV and filters the tehsil list to those with `version` in `[min_version, max_version]`. These are the only tehsils for which attribute data will be downloaded.

If `active_locations` is omitted, tehsil filtering is skipped and only the base layer is processed.

---

## Step 4 — Base layer fetch

The worker reads the base layer from the `source` path in the base `[[layers]]` entry:

1. Tries `scan_parquet` directly. If that fails (GeoJSON, Shapefile, etc.) it converts via DuckDB `ST_Read`, applies a **Hilbert curve spatial sort** (`ORDER BY ST_Hilbert(geom)`), and writes ZSTD-compressed Parquet.
2. If `super_layer_source` + `super_field` are set, a **centroid-in-polygon join** tags each base entity with its super-layer region (e.g. basin name) during this conversion step.

The descriptor's `rename`, `drop`, and `scale` for the base layer are then applied, and the geometry column is standardised to `geometry` (WKB, EPSG:4326).

---

## Step 5 — Parallel tehsil downloads

For every `type = "collection"` layer, the worker enqueues one download task per filtered tehsil onto the `meta` Redis queue. Each task runs `download_and_convert_geojson`, which:

1. Fills `{district}` and `{tehsil}` slugs (lowercase, non-alphanumeric → `_`) into the layer's `url_template`.
2. Fetches the GeoJSON from GeoServer via HTTP.
3. Applies `rename_and_drop` and `apply_scaling` from the layer descriptor.
4. Tags every row with `state`, `district`, and `tehsil` labels.
5. Writes ZSTD Parquet to `{temp_path}/{layer}_{district}_{tehsil}.parquet`.

The orchestrator polls every 5 seconds until all tasks finish or fail, then lazy-scans each layer's temp files as a glob and concatenates them.

---

## Step 6 — Per-layer post-processing

For each attribute layer result, the pipeline applies in order:

| Step | Function | What it does |
|---|---|---|
| Range splitting | `split_cols` | String range columns like `"30 - 200"` → `col_min` / `col_max` Float64. |
| JSON unnesting | `unnest_json_cols` | Prefix+year columns (e.g. `dw_2019_2020`) containing JSON dicts → individual Float64/String columns. |
| Column prefixing | `prefix_cols` | Adds 2-char layer prefix (e.g. `te_` for `terrain`) to all non-common columns to avoid merge collisions. |

Each layer is then sunk to a temp Parquet and re-scanned lazily to break the Polars query plan.

---

## Step 7 — Merge

`merge_all_layers` left-joins every attribute layer onto the base on `entity_key` (e.g. `mws_id`):

- Join key is cast to `Utf8` across all frames to prevent type mismatches.
- Location metadata (`state`, `district`, `tehsil`) is extracted from the first attribute layer that contains it and joined onto the base before the rest.
- Each layer is deduplicated on `entity_key` (entities that span multiple tehsil files appear in multiple slices).

Base entities with no matching attribute data receive `null` values. The merged frame is sunk to `merged.parquet` to materialise the join plan.

---

## Step 8 — Admin boundary fill (optional)

If any entity has null `state`/`district`/`tehsil` after the merge (outside all active tehsils), and `add_admin = true` or missing rows exist, the pipeline runs a polygon-intersection join against the tehsil boundary shapefile (`settings.tehsil_bounds`). Processed in batches of 25,000 to avoid DuckDB OOM.

This correctly handles large entities (like watersheds) that span multiple boundaries. The `state`, `district`, and `tehsil` columns are converted to `List[String]` types across the entire dataset, containing all overlapping administrative zones for each entity.

---

## Step 9 — Column classification and output

`classify_columns` splits every merged column into buckets:

| Bucket | Rule |
|---|---|
| **Static** | `COMMON_COLS` (`mws_id`, `geometry`, `tehsil`, `district`, `state`, `area_in_ha`), configured `entity_key`, and any unrecognised columns. |
| **Fortnightly** | Name ends with ISO date (`YYYY-MM-DD`). |
| **Annual** | Name ends with year or year-range (`2023` or `2019_2020`). |
| **Dropped** | Name contains `"net"` (derived data). |

Three output directories are written under `output_path`:

```
output_path/
├── static/          # GeoParquet 1.1.0 with bbox struct
├── fortnightly/     # Long Parquet — one row per (entity_key, date)
│   └── year=YYYY/
└── annual/          # Long Parquet — one row per (entity_key, year)
    └── year=YYYY/
```

Before being finalised, all output `.parquet` files are post-processed to inject **Bloom filters** for the primary entity key and all string columns (to accelerate point-lookups). Output filenames are also normalised to `data_N.parquet` (e.g. `data_0.parquet`) within each partition directory.

If `partition_by` is set, an outer Hive level wraps all three (e.g. `ba_name=Cauvery/`). Temp files are cleaned up and the job is marked **finished**.

---

## Status Polling

`GET /api/v1/status?task_id={task_id}` to check if the job is `queued`, `started`, `finished`, or `failed`.

## Flow Diagram

```{mermaid}
sequenceDiagram
    participant Client
    participant API (FastAPI)
    participant Redis (Queue)
    participant Worker (RQ)
    participant Storage (S3/Local)

    Client->>API: POST /api/v1/vector/layers (url, output)
    API->>Redis: Enqueue Job
    API-->>Client: Returns task_id & status

    Worker->>Redis: Polls for job
    Redis-->>Worker: Assigns Job

    Worker->>Storage: Fetch & Parse TOML Descriptor
    Storage-->>Worker: Descriptor Content
    Note over Worker: Builds LayerConversionRequest<br/>from TOML, then runs pipeline<br/>(DuckDB, Polars, polars-st)

    Worker->>Storage: Fetch Raw Geodata
    Storage-->>Worker: Geodata

    Worker->>Storage: Save Processed Data (output_path)

    Client->>API: GET /api/v1/status?task_id={id}
    API->>Redis: Check Job Status
    Redis-->>API: Status (Finished)
    API-->>Client: Return Status (Finished)
```
