# CoREStack GeoData Converter

A high-performance pipeline for converting and merging CoREStack spatial data into cloud-native formats like GeoParquet.

## Features

- **Asynchronous Data Fetching**: Pulls GeoJSON layers from GeoServer concurrently using `asyncio` and `rq` queues.
- **Batched Spatial Joins**: Uses DuckDB's spatial extension for fast, memory-efficient point-in-polygon joins to assign administrative boundaries.
- **Cloud-Native Output**: Writes to partitioned GeoParquet 1.1.0 (by state) optimized for analytical queries (ZSTD compression, row groups).
- **Data Deduplication & Normalization**: Handles overlapping polygons, range-based strings (e.g. "30 - 200"), and nested JSON columns automatically.

## Current Focus

### Layers Covered
- Pan-India MWS (Micro-Watershed) Base Layer
  - Aquifer
  - Annual Water Balance
  - Fortnightly Water Balance
  - Terrain Clusters
  - Cropping Intensity
  - SOGE

## Workflow Architecture

1. **Initialization**: Fetches active tehsils and the authoritative layer version metadata.
2. **Base Conversion**: Downloads the base MWS layer and converts it to Parquet for fast processing.
3. **Layer Fetching**: Queues tasks to download GeoJSONs for each requested layer per active tehsil.
4. **Processing**: Merges tehsil-level GeoJSONs, cleans/normalizes columns, and splits complex attributes (ranges, JSON).
5. **Merging**: Left-joins all processed layers onto the base MWS dataset.
6. **Admin Fill**: Performs a DuckDB-powered spatial join to assign missing state/district/tehsil data for polygons spanning boundaries.
7. **Sink**: Partitions the merged data by state and writes GeoParquet 1.1.0 files with correct global bounding box metadata.
