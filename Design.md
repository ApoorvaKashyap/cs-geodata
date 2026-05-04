---
title: Design.md

---

# GeoData Pipeline — Design Document

**Status:** Pre-implementation design
**Domain:** Geospatial ETL for ecolib dataset generation
**Primary Source:** CoreStack GeoServer (WFS) and S3
**Primary Sink:** ecolib-compatible GeoParquet / Parquet directory tree
**Last Updated:** Architecture session complete

---

## Changelog

### v2 (Architecture session)
- Complete ground-up redesign — sink-shaped rather than source-shaped
- Descriptor-driven architecture — TOML files declare what to build, pipeline is a general engine
- STAC catalogue integration — schema, CRS, and asset location read from live CoreStack catalogue
- Two source types only: `WFSSource` (tehsil-wise GeoServer layers) and `S3Source` (Pan India files)
- Admin gap-fill removed — admin attribution is the library's responsibility at query time
- Column prefixing removed — each output file is scoped to a single entity, collisions impossible
- DuckDB retained at write boundary only — partitioned GeoParquet via `COPY ... PARTITION BY`
- Descriptor validation at API layer — schema checked against live STAC before any data is fetched
- `POST /runs` accepts multiple Descriptor URIs — all entities processed in parallel, fully independent
- `partition_by` declared per Descriptor — varies by entity (river basin for MWS, state for villages)
- Progress reporting structured — Redis-backed per-run state, not log scraping

### v1 (Original pipeline)
- Single-entity (MWS only), source-shaped design
- Hardcoded tehsil fetch index from CoreStack active locations API
- Column prefixes (`te_`, `aq_`, `so_`) to avoid merge collisions
- Wide temporal columns — not melted to long format
- Admin gap-fill via centroid-in-polygon (DuckDB spatial join)
- State-partitioned GeoParquet as primary output
- RQ with four named queues (`base`, `id`, `layers`, `meta`)

---

## 1. Problem Statement

The ecolib library requires a structured directory tree of GeoParquet and Parquet files, one directory per entity (mws, tehsil, district, state, village, forest patch, etc.), each containing `static.geoparquet` and optionally `annual.parquet` and `fortnightly.parquet`. This data originates from CoreStack — a platform that serves vector geospatial layers via a GeoServer WFS API (for tehsil-partitioned data) and S3 (for Pan India boundary and reference files).

The original pipeline was built exclusively for MWS data and cannot produce the full ecolib directory tree without significant rework. It has additional structural problems: it encodes MWS-specific logic throughout, column naming conventions conflict with ecolib's conventions, temporal data is not reshaped to the long format ecolib expects, and admin attribution is baked in at the pipeline layer when it belongs in the library.

The goal of this redesign is a general-purpose ETL engine that can produce any ecolib entity dataset from a declarative description, with no code changes required to add new entities or new layers.

---

## 2. Core Design Philosophy

- **Sink-shaped, not source-shaped.** The pipeline is built around what ecolib needs, not around how CoreStack serves data. Fetching adapts to the source; everything downstream is driven by the output contract.
- **Declarative over imperative.** Domain knowledge lives in TOML Descriptor files, not in Python functions. Adding a new entity or a new layer is a Descriptor edit, not a code change.
- **STAC as the source of truth.** Schema, CRS, and asset location come from the live CoreStack STAC catalogue at run time. The pipeline never hardcodes column names or URLs.
- **Each entity is fully independent.** No entity's processing depends on another entity's output. All Descriptors in a run execute in parallel.
- **Fail loudly, fail early.** All validation — schema, rename declarations, column existence — happens at API time before any data is fetched.
- **No spatial computation.** The pipeline performs no spatial joins, no centroid-in-polygon, no geometry creation. It is pure ETL. Spatial relationships are the library's responsibility.
- **DuckDB only at the write boundary.** DuckDB handles partitioned GeoParquet writes via its spatial extension and `COPY ... PARTITION BY`. All other processing is Polars.

---

## 3. Separation of Concerns

### API Layer — Validation and Enqueue
- Accept `POST /runs` with Descriptor URIs and output root
- Fetch each referenced STAC item from the live CoreStack catalogue
- Validate Descriptor's `rename` and `drop` declarations against STAC `table:columns`
- Validate `partition_by` column presence in declared layers
- Reject with structured field-level errors before enqueuing anything
- Expose `GET /runs/{run_id}` for structured progress (not log scraping)
- Expose `POST /descriptors/validate` for authoring-time validation without starting a run

### Worker Layer — Orchestration
- One RQ job per Descriptor
- Sequences pipeline stages: Fetch → Normalise → Reshape → Write
- Writes structured progress to Redis at each stage transition
- Isolated working directory per run (`/tmp/runs/{run_id}/`) — cleaned up on completion or failure

### Pipeline Stages
Each stage is a pure function: takes a Polars LazyFrame in, returns a Polars LazyFrame out. Stages have no knowledge of which entity or source they are processing.

**Fetch** — pulls raw GeoJSON or Parquet from the source, concatenates into a single LazyFrame. The only stage that touches the network. Parallelised across tehsils for WFS sources.

**Normalise** — applies renames, drops, and type coercions from the Descriptor. Emits clean column names. No prefixing.

**Reshape** — branches on `resolution`. Static frames pass through unchanged. Annual and fortnightly frames are melted from wide (`ci_2018_2019`, `ci_2019_2020`) to long (`year`, `ci`) using the Descriptor's `temporal_pattern`. The melt key and value column names come from the Descriptor, not from pattern matching heuristics.

**Write** — for static layers: registers the LazyFrame as a DuckDB Arrow view, runs `COPY ... PARTITION BY (partition_col)` with `ST_SetCRS` inline, then patches GeoParquet 1.1 metadata (global bbox, `covering` declaration) via pyarrow. For temporal layers: `polars.sink_parquet()` directly — no geometry, no DuckDB.

### Source Layer — Data Acquisition
Two source types. Both emit a Polars LazyFrame with raw source columns.

**WFSSource** — for tehsil-partitioned GeoServer layers. Iterates over the active tehsil list (derived from the STAC catalogue hierarchy), constructs GeoServer URLs, fetches GeoJSON per tehsil in parallel, concatenates.

**S3Source** — for Pan India files. Downloads a single object from S3, reads to LazyFrame. No iteration.

---

## 4. The Descriptor

The Descriptor is the central design artefact. It is a TOML file that declares everything needed to produce one ecolib entity dataset. It is authored by hand and stored in version control or S3.

### 4.1 Fields

```toml
entity = "mws"            # ecolib entity name — determines output directory
key = "mws_id"            # primary key column after rename
geometry = "geometry"     # geometry column name
partition_by = "river_basin"  # column used for GeoParquet partitioning

[[layers]]
name = "terrain"                    # logical name, used in progress reporting
resolution = "static"               # "static" | "annual" | "fortnightly"
stac_item = "https://..."           # URI to the live STAC item JSON

[layers.rename]
uid = "mws_id"
terrainClu = "terrain_cluster"

drop = ["area_in_ha"]

[[layers]]
name = "cropping_intensity"
resolution = "annual"
stac_item = "https://..."
temporal_pattern = "{col}_{YYYY}_{YYYY}"   # how to parse year from wide column names

[layers.rename]
uid = "mws_id"
```

### 4.2 What Comes From the Descriptor

- Entity name, key column, geometry column, partition column
- Layer names, resolutions, temporal patterns
- Column renames and drops
- STAC item URIs (which resolve to schema, CRS, and asset location at run time)

### 4.3 What Comes From the STAC Catalogue at Run Time

- Source column names and types (from `table:columns`) — used for rename validation
- CRS (from `proj:code`) — applied at write time
- Asset download location (from `assets` href) — determines Source type and fetch parameters

The pipeline never hardcodes column names, URLs, or CRS values. The Descriptor declares intent; the STAC item resolves it.

### 4.4 Source Type Inference

The pipeline infers Source type from the STAC asset href:

- GeoServer WFS URL → `WFSSource` — fetches per tehsil, parallelised
- S3 URI (`s3://...`) → `S3Source` — single download

No `type` field is required in the Descriptor.

---

## 5. API

### Endpoints

**`POST /runs`**
```json
{
  "descriptors": [
    "s3://bucket/descriptors/mws_v1.toml",
    "s3://bucket/descriptors/tehsil_v1.toml",
    "s3://bucket/descriptors/village_v1.toml"
  ],
  "output_root": "s3://bucket/ecolib_data/"
}
```
Returns `{ "run_id": "uuid" }`. Validates all Descriptors against live STAC before enqueuing. Rejects with field-level errors if any Descriptor is invalid.

**`GET /runs/{run_id}`**
```json
{
  "run_id": "uuid",
  "status": "running",
  "descriptors": [
    {
      "entity": "mws",
      "status": "running",
      "phase": "fetch",
      "layer": "cropping_intensity",
      "progress": { "completed": 3200, "total": 5000 }
    },
    {
      "entity": "tehsil",
      "status": "complete"
    }
  ]
}
```

**`POST /descriptors/validate`**
```json
{ "descriptor_uri": "s3://bucket/descriptors/mws_v1.toml" }
```
Returns the validated Descriptor echoed as JSON, or structured validation errors. Does not enqueue a run. Intended for use during Descriptor authoring.

---

## 6. Output Structure

The pipeline writes directly to ecolib's expected directory tree:

```
{output_root}/
  mws/
    static.geoparquet          # partitioned by river_basin
    annual.parquet
    fortnightly.parquet
  tehsil/
    static.geoparquet          # partitioned by state
  district/
    static.geoparquet
  state/
    static.geoparquet
  village/
    static.geoparquet          # partitioned by state
    annual.parquet
```

### Static GeoParquet

- Partitioned by the Descriptor's `partition_by` column
- Written via DuckDB `COPY ... PARTITION BY` with `ST_SetCRS` — streams through data partition-by-partition, bounded peak memory regardless of dataset size
- pyarrow post-processing injects global bbox and GeoParquet 1.1 `covering` declaration
- CRS sourced from STAC `proj:code` at write time

### Temporal Parquet

- Long format: `(key, year, col1, col2, ...)` for annual; `(key, date, col1, col2, ...)` for fortnightly
- Written via `polars.sink_parquet()` — no geometry, no DuckDB
- Wide-to-long reshape driven by Descriptor's `temporal_pattern`

---

## 7. Temporal Reshaping

Source data from CoreStack uses wide temporal columns: `ci_cropping_intensity_2018_2019`, `ci_cropping_intensity_2019_2020`, `deltaG_fortnight_2021_001`, etc.

ecolib expects long format. The Descriptor's `temporal_pattern` field declares how to parse the time dimension from wide column names:

| `temporal_pattern` | Example source column | Parsed key | Example output columns |
|---|---|---|---|
| `{col}_{YYYY}_{YYYY}` | `ci_2018_2019` | `year = 2018` | `(mws_id, year, ci)` |
| `{col}_{YYYY}_{NNN}` | `deltaG_2021_001` | `year = 2021, fortnight = 1` | `(mws_id, year, fortnight, deltaG)` |

The reshape stage uses the pattern to identify which columns are temporal, extract the time key, and melt the frame. The pattern is per-layer, not per-pipeline — different layers in the same entity can have different temporal structures.

---

## 8. Concurrency Model

### Queue Structure

Two RQ queues:

**`runs`** — one job per Descriptor per run. The orchestrator for a single entity. Sequences Fetch → Normalise → Reshape → Write.

**`fetch`** — one job per tehsil per WFS layer. Fired by the runs worker during the Fetch stage. Results accumulated and concatenated by the runs worker.

### Run Isolation

Each pipeline run gets a UUID-scoped working directory: `/tmp/runs/{run_id}/`. All intermediate files for that run live there. Cleanup is registered as a completion hook — runs on both success and failure. Concurrent runs for different entities (or different runs entirely) cannot interfere.

### Progress

The runs worker writes structured progress to Redis at each stage transition:

```
runs:{run_id}:mws → {phase, layer, completed_fetches, total_fetches, status}
```

`GET /runs/{run_id}` reads directly from Redis. No log scraping, no polling files.

---

## 9. Validation Rules

All rules are enforced at `POST /runs` time, before enqueuing:

- Every `stac_item` URI must resolve to a valid STAC item JSON
- Every column named in `rename` keys must exist in the STAC item's `table:columns`
- Every column named in `drop` must exist in the STAC item's `table:columns`
- The `key` column (after rename) must be present across all layers in the Descriptor
- The `partition_by` column must be present in at least one declared static layer
- For temporal layers, `temporal_pattern` must be present and must match at least one column in `table:columns`
- Duplicate entity names across Descriptors in a single run are rejected

Validation errors are returned as a structured list with Descriptor URI, layer name, field name, and error message. Nothing is enqueued until all Descriptors in the request pass validation.

---

## 10. Source Layout

```
geodata_pipeline/
├── main.py                          # FastAPI entrypoint
├── worker.py                        # RQ worker entrypoint
├── descriptors/                     # versioned Descriptor TOML files
│   ├── mws_v1.toml
│   ├── tehsil_v1.toml
│   ├── district_v1.toml
│   ├── state_v1.toml
│   └── village_v1.toml
└── src/
    ├── api/
    │   ├── routes.py                # POST /runs, GET /runs/{id}, POST /descriptors/validate
    │   └── models.py                # RunRequest, RunStatus, DescriptorStatus
    ├── descriptor/
    │   ├── schema.py                # Pydantic models: EntityDescriptor, LayerDescriptor
    │   └── validator.py             # validates Descriptor against live STAC item
    ├── stac/
    │   └── client.py                # fetches STAC items, returns schema + source info
    ├── sources/
    │   ├── base.py                  # AbstractSource
    │   ├── wfs.py                   # WFSSource — parallel per-tehsil GeoJSON fetch
    │   └── s3.py                    # S3Source — single object download
    ├── pipeline/
    │   ├── orchestrator.py          # sequences stages, writes progress
    │   ├── fetch.py                 # dispatches to correct Source, concatenates
    │   ├── normalise.py             # rename, drop, type coercions
    │   ├── reshape.py               # wide-to-long melt for temporal layers
    │   └── write.py                 # DuckDB GeoParquet writer + pyarrow metadata patch
    ├── progress/
    │   └── store.py                 # Redis-backed structured run progress
    └── utils/
        ├── config.py                # pydantic-settings
        ├── workspace.py             # UUID-scoped /tmp run directories
        └── duckdb.py                # configured DuckDB connection factory (spatial ext, memory limit)
```

---

## 11. Technology Stack

| Concern | Library |
|---|---|
| API and request validation | FastAPI, Pydantic |
| Job queue | RQ + Redis |
| Data processing, reshape, concatenation | Polars |
| Partitioned GeoParquet write with CRS | DuckDB (spatial extension) |
| GeoParquet 1.1 metadata (bbox, covering) | pyarrow |
| S3 I/O | fsspec + s3fs |
| STAC catalogue reads | httpx (async) |
| Configuration | pydantic-settings |

---

## 12. Design Decisions and Rationale

### Why TOML for Descriptors?
TOML natively supports dicts and lists, allows comments, has no indentation ambiguity, and is part of the Python standard library from 3.11 via `tomllib`. It is more ergonomic for hand-authoring than JSON and more strict than YAML. Pydantic reads it directly after `tomllib.loads()`.

### Why not validate Descriptors from a local STAC snapshot?
The crawled STAC catalogue is useful for Descriptor authoring and exploration, but it can drift from the live catalogue as CoreStack adds or modifies layers. The pipeline always fetches STAC items fresh at run time so that schema validation and asset URLs are current.

### Why DuckDB only at the write boundary?
DuckDB's `COPY ... PARTITION BY` streams through data partition-by-partition via its Arrow integration with Polars, keeping peak memory proportional to the largest partition rather than the full dataset. This is the correct tool for writing large partitioned GeoParquet. Everything upstream is pure Polars LazyFrame operations, which are easier to test and reason about without a SQL engine.

### Why no admin gap-fill in the pipeline?
Admin attribution (which tehsil does this MWS belong to?) involves a spatial join and is correctly the library's responsibility at query time. Baking it into the pipeline created a tight coupling between the MWS processing logic and the tehsil boundary data, required a DuckDB centroid-in-polygon pass mid-pipeline, and produced output that conflicted with ecolib's own naming conventions for spatial join columns. Removing it simplifies the pipeline and produces cleaner output.

### Why is each Descriptor fully independent?
Entity datasets in ecolib are joined by the library at query time via spatial intersection, not by the pipeline. There are no cross-entity dependencies at ETL time. Full independence means all Descriptors in a run can execute in parallel with no coordination, and a failure in one entity's processing does not affect others.

### Why infer Source type from the STAC asset href?
Requiring an explicit `type = "wfs"` or `type = "s3"` field in the Descriptor is redundant — the asset href already encodes this information unambiguously. Inferring it removes a class of authoring errors (wrong type declared for a given href) and keeps the Descriptor focused on the ecolib-specific transformations that the author actually needs to specify.

---

## 13. What Stays the Same From v1

- **Polars for all data processing.** The boundary with DuckDB is the same: Polars everywhere except the GeoParquet write.
- **Intermediate materialisation before wide joins.** Sinking to a temp Parquet before joining frames with many columns avoids OOM on Polars lazy plans. Retained where applicable.
- **GeoParquet 1.1 metadata patching via pyarrow.** DuckDB writes correct per-partition bboxes but does not write a global bbox or `covering` declaration. The pyarrow post-processing step is retained.
- **pydantic-settings for configuration.** Unchanged.
- **RQ + Redis for job queue.** Retained with simplified queue structure (two queues instead of four).
- **UUID-scoped run directories.** Introduced from v1's implicit `/tmp` usage, now formalised.

---

## 14. Out of Scope

- Raster data ingestion — pipeline handles vector data only
- Data from sources other than CoreStack GeoServer WFS and S3
- Cross-entity spatial joins at ETL time — this is the library's responsibility
- Admin attribution (state, district, tehsil columns on MWS rows) — library's responsibility
- Incremental updates — pipeline produces full dataset snapshots; diffing and patching are out of scope
- Data quality validation beyond schema conformance — the pipeline trusts CoreStack data
- Authentication to CoreStack beyond what is already configured — assumed handled at infrastructure level

---

## 15. First Milestone

The natural first working slice is a single static, Pan India entity end-to-end:

1. Write a Descriptor for `tehsil` pointing to the tehsil boundaries STAC item (S3 source, static resolution, no temporal complexity)
2. `POST /runs` with that Descriptor URI
3. Validation fetches the live STAC item, confirms `TEHSIL`, `District`, `STATE` columns are present, confirms the S3 asset URI resolves
4. Worker downloads from S3, normalises column names, writes `tehsil/static.geoparquet` partitioned by state
5. pyarrow patches GeoParquet metadata

This exercises the full pipeline path — API, validation, queue, fetch, normalise, write — with the simplest possible case. WFS fetching and temporal reshaping are introduced in subsequent milestones.
