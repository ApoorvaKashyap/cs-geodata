# Descriptor TOML Reference

Every pipeline run is driven by a single TOML file — the **descriptor**. It tells the system what entity to build, which base layer to use, which attribute layers to join, and how to clean each one.

Pass it to the API via `POST /api/v1/vector/layers` as the `descriptor_url` field (S3 URI, HTTPS URL, or GitHub raw URL).

## Top-Level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `entity` | string | ✅ | Logical name for the base unit (e.g. `"mws"`, `"village"`). |
| `key` | string | ✅ | Join key column shared across all layers (e.g. `"mws_id"`). |
| `geometry` | string | | Geometry column name. Default: `"geometry"`. |
| `base` | string | ✅ | Name of the `[[layers]]` entry that is the base geometry. |
| `active_locations` | string | | URL to a CSV mapping tehsils to version numbers. Omit to process all tehsils without version filtering. Also accepted as `layer_version`. |
| `min_version` | float | | Minimum layer version to include (inclusive). Default: `0.0`. |
| `max_version` | float | | Maximum layer version to include (inclusive). Default: `9999.0`. |
| `super_layer_source` | string | | Path or S3 URI to a boundary file used for hierarchical partitioning (e.g. basin boundaries). Must be paired with `super_field`. |
| `super_field` | string | | Column inside the super-layer file whose value is assigned to every base entity via centroid-in-polygon join. Must be paired with `super_layer_source`. |
| `super_layer_key` | string | | Informational key column in the super-layer (not used in joins). |
| `partition_by` | string | | Output Hive partition column. Usually the same as `super_field`. |
| `add_admin` | bool | | If `true`, runs a polygon-intersection join against tehsil boundaries to fill `state`/`district`/`tehsil` (as lists of strings) for any rows with null admin columns. Default: `false`. |

```toml
entity = "mws"
key = "mws_id"
base = "mws_boundaries"
min_version = 1.1
max_version = 2
active_locations = "s3://my-bucket/metadata/layer_version.csv"

super_layer_source = "s3://my-bucket/stac_pan_india/basin.geojson"
super_field = "ba_name"
partition_by = "ba_name"
add_admin = true
```

---

## `[[layers]]` Entries

Each layer is declared as an array-of-tables entry. One entry must match the `base` field at the top level; all others are attribute layers.

### Common Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | ✅ | Unique layer identifier. The base layer `name` must match the top-level `base` field. |
| `type` | string | ✅ | `"item"` — single pan-India file. `"collection"` — tehsil-partitioned WFS. |
| `source` | string | (`"item"` only) | Path or S3 URI to the source file. |
| `url_template` | string | (`"collection"` only) | WFS URL template with `{district}` and `{tehsil}` placeholders. |
| `stac_item` | string | | STAC item URL, used for schema/CRS inference. |
| `sample_item` | string | | STAC collection URL, used for schema preview. |
| `drop` | list[string] | | Column names to drop after reading. |
| `rename` | table | | Column rename map. Supports glob patterns (e.g. `k_* = "kharif_*"`). Case-insensitive. |
| `scale` | table | | Map of column name (or glob) to a float multiplication factor. |
| `resolution` | string | | Temporal resolution hint. Set to `"fortnightly"` for fortnightly layers. |

### `type = "item"` — Base / Pan-India Layer

Use for a single file that covers all locations (typically the base geometry layer).

```toml
[[layers]]
name = "mws_boundaries"
type = "item"
source = "s3://my-bucket/base_converted/mws_basins.parquet"
drop = ["id"]

[layers.rename]
uid = "mws_id"
geom = "geometry"
```

### `type = "collection"` — WFS Attribute Layer

Use for layers served via GeoServer WFS, partitioned by district and tehsil. The system downloads one GeoJSON slice per active tehsil in parallel.

```toml
[[layers]]
name = "terrain"
type = "collection"
url_template = "https://geoserver.example.org/geoserver/terrain/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=terrain%3A{district}_{tehsil}_cluster&outputFormat=application%2Fjson"
drop = ["area_in_ha", "id"]

[layers.rename]
uid = "mws_id"
terrainClu = "terr_cluster_id"
geom = "geometry"
```

The `{district}` and `{tehsil}` placeholders are filled with slug-form names (lowercase, non-alphanumeric → `_`).

### Glob Patterns in `rename` and `scale`

Both `rename` and `scale` support `*` as a wildcard:

```text
[layers.rename]
k_* = "kharif_*"   # k_2018 → kharif_2018, k_2019 → kharif_2019

[layers.scale]
"area_*" = 0.0001  # converts m² to ha for any column starting with area_
```

---

## Output Structure

The pipeline writes three output directories under `output_path`:

```
output_path/
├── static/          # GeoParquet — geometry + identity + non-temporal columns
├── fortnightly/     # Long Parquet — one row per (entity_key, date)
│   └── year=YYYY/
└── annual/          # Long Parquet — one row per (entity_key, year)
    └── year=YYYY/
```

If `partition_by` is set, an outer Hive partition level is added:

```
static/ba_name=Cauvery/part-0.parquet
annual/ba_name=Cauvery/year=2023/part-0.parquet
```

---

## Full Example

See [`examples/mws.toml`](https://github.com/ApoorvaKashyap/cs-geodata/blob/main/examples/mws.toml) for a complete working descriptor.

For a step-by-step trace of how a descriptor drives a full pipeline run, see [Data Flow](data_flow.md).
