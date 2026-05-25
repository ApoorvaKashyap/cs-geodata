# CoREStack GeoData Converter

A high-performance pipeline for converting and merging CoREStack spatial data into cloud-native formats like GeoParquet.

## Features

- **Asynchronous Data Fetching**: Pulls GeoJSON layers from GeoServer concurrently using `asyncio` and `rq` queues.
- **Batched Spatial Joins**: Uses DuckDB's spatial extension for fast, memory-efficient point-in-polygon joins to assign administrative boundaries.
- **Cloud-Native Output**: Writes to partitioned GeoParquet 1.1.0 (by state) optimized for analytical queries (ZSTD compression, row groups).
- **Data Deduplication & Normalization**: Handles overlapping polygons, range-based strings (e.g. "30 - 200"), and nested JSON columns automatically.

## Workflow Architecture

1. **Initialization**: Fetches active tehsils and the authoritative layer version metadata.
2. **Base Conversion**: Downloads the base MWS layer and converts it to Parquet for fast processing.
3. **Layer Fetching**: Queues tasks to download GeoJSONs for each requested layer per active tehsil.
4. **Processing**: Merges tehsil-level GeoJSONs, cleans/normalizes columns, and splits complex attributes (ranges, JSON).
5. **Merging**: Left-joins all processed layers onto the base MWS dataset.
6. **Admin Fill**: Performs a DuckDB-powered spatial join to assign missing state/district/tehsil data for polygons spanning boundaries.
7. **Sink**: Partitions the merged data by state and writes GeoParquet 1.1.0 files with correct global bounding box metadata.

## Usage

### 1. Configuration
The pipeline is driven by a configuration descriptor which can be defined in TOML or JSON format. See `examples/mws.toml` for a complete example.

Key parameters in the configuration:
- `entity`: The base entity name (e.g., `mws`).
- `base`: The base boundaries layer source.
- `active_locations`: S3 path to the layer version metadata CSV.
- `layers`: A list of attribute/temporal layers to merge. Each layer specifies its `type` (e.g., `collection`), `url_template`, columns to `drop`, and a `rename` mapping.

### 2. Running the Pipeline
You can run the pipeline by loading your JSON configuration and passing it into the `run_mws_pipeline` function. Ensure your Redis/RQ workers are running if processing WFS layers concurrently.

```python
import asyncio
import json
from src.app.models import LayerConversionRequest
from src.conversion.algos import run_mws_pipeline
from loguru import logger

if __name__ == "__main__":
    logger.add("logs/mws.log")

    # Load configuration
    with open("examples/mws.json") as f:
        config_data = json.load(f)

    request = LayerConversionRequest(**config_data)

    # Run pipeline
    asyncio.run(run_mws_pipeline(request))
```

### 3. Output
The pipeline produces three output types partitioned by your specified `partition_by` column (e.g., `sub_basin`):
- `static/`: Base geometry and non-temporal attributes (GeoParquet).
- `fortnightly/`: Melted temporal data (if applicable), partitioned by year.
- `annual/`: Melted annual data (if applicable), partitioned by year.
