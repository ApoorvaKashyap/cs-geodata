# Base Layer Pipeline

Before attribute layers can be processed and joined, the system needs a structured, optimized **Base Layer** (the core geographic entities like microwatersheds, villages, or tehsils).

The Base Layer pipeline (triggered via `/api/v1/vector/create_base_cache`) runs several specific spatial transformations and checks to ensure the base data is highly optimized for downstream queries and joins.

## 1. Format Standardization
Raw geographic data (which can be GeoJSON, Shapefile, etc., from local storage or S3) is ingested and standardized.
* **Process**: The data is read using DuckDB's spatial extension (`ST_Read`). Geometries are explicitly cast to Well-Known Binary (WKB) format to ensure compatibility with downstream tools like Polars.

## 2. Spatial Sorting (Hilbert Curve)
To drastically improve spatial query performance and file compression, the base layer undergoes spatial sorting.
* **Process**: The system applies an `ORDER BY ST_Hilbert(geom)` operation.
* **Why**: A Hilbert curve is a continuous fractal space-filling curve. Sorting geographic data along this curve ensures that features physically close to each other on the Earth are also stored close to each other on disk. This maximizes the efficiency of Parquet row-groups during spatial bounding-box queries.

## 3. Super-Layer Spatial Join (Enrichment)
Often, a base entity (like a microwatershed) needs to be tagged with the region it belongs to (like a river basin) for partitioning purposes.
* **Process**: If a `super_layer_source` (e.g., basin boundaries) and a `super_field` (e.g., basin name) are provided, the system performs a **centroid-in-polygon spatial join**.
* **Logic**: `ST_Within(ST_Centroid(base_geometry), super_geometry)`.
* This efficiently assigns the corresponding super-layer attribute to every base entity without complex intersection math.


## 4. Optimized Export
* **Process**: The sorted and enriched base layer is exported directly to a Parquet file with `ZSTD` compression. This final Parquet file acts as the primary geographic index for all subsequent attribute layer processing (the main vector conversion pipeline).
