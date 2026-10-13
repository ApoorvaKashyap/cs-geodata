# Configuration & Settings

`cs-geodata` uses [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to manage configuration. All settings can be provided as environment variables or loaded from a `.env` file in the project root.

## Precedence

Settings are loaded in the following order of precedence (highest to lowest):

1. **Environment variables** (e.g., `export REDIS_HOST=...`)
2. **`.env` file** in the project root
3. **Default values** defined in the `Settings` class

## Environment Variables

Here is the complete list of available settings. Variables that represent secrets use Pydantic's `SecretStr` to ensure they are never accidentally logged.

### Redis Configuration
| Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `"localhost"` | Hostname or IP address of the Redis server. |
| `REDIS_PORT` | `6379` | Port number of the Redis server. |

### AWS / S3 Configuration
| Variable | Default | Description |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | `""` | AWS access key for S3 integration. |
| `AWS_SECRET_ACCESS_KEY` | `""` | AWS secret key for S3 integration (Secret). |
| `AWS_REGION` | `"ap-south-1"` | Default AWS region. |
| `S3_BASE` | `""` | Base S3 path or bucket used across the application. |

### CoREStack API Integration
*(Note: CoREStack API fetching has been replaced by S3 version manifests. These settings are currently unused.)*
| Variable | Default | Description |
|---|---|---|
| `CORESTACK_API_URL` | `""` | Base URL for the CoREStack API (Unused). |
| `CORESTACK_API_KEY` | `""` | Authentication key for the CoREStack API (Secret, Unused). |
| `BASE_GEOSERVER` | `""` | Base URL for the WFS GeoServer. |

### File Paths
| Variable | Default | Description |
|---|---|---|
| `TEMP_PATH` | `"/tmp/"` | Directory used for temporary files (e.g. downloaded Parquet files). |
| `TEHSIL_BOUNDS` | `""` | Path or S3 URI pointing to the raw tehsil boundaries shapefile/geojson. Used for administrative boundary filling. |

### DuckDB Settings
| Variable | Default | Description |
|---|---|---|
| `DUCKDB_MEMORY_LIMIT` | `"12GB"` | Maximum memory DuckDB is allowed to use during complex spatial joins. |
| `DUCKDB_THREADS` | `4` | Number of threads DuckDB is allowed to use. |
| `DUCKDB_TEMP_DIR` | `"/tmp/duckdb_spill"` | Directory DuckDB uses to spill data to disk when memory limits are exceeded. |

### Parquet Output Settings
| Variable | Default | Description |
|---|---|---|
| `PARQUET_ROW_GROUP_SIZE` | `100000` | Target row group size for the output Parquet files. |
| `PARQUET_COMPRESSION_LEVEL` | `15` | ZSTD compression level (DuckDB default is 3, up to 22). |

### Debugging / Development
| Variable | Default | Description |
|---|---|---|
| `TEST_LIMIT_TEHSILS` | `None` | If set to an integer, limits the number of tehsils processed per layer during a pipeline run. Useful for testing pipelines locally without downloading pan-India data. |
