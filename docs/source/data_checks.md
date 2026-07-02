# Data Checks and Cleaning

The `cs-geodata` system performs extensive cleaning and validation checks on incoming vector data. This ensures that the data is standardized before it is processed or exported.

The core cleaning operations are defined in the data transformation pipeline (specifically `src/conversion/helpers/cleaners.py`).

## Execution Flow

The data transformations are applied in a specific order during the lifecycle of a job:

1. **Initial Extraction**: As soon as raw data is fetched (via API or Storage), **Column Renaming and Dropping** is executed to immediately filter and normalize column names. During this initial stage, **Label Normalization** is also applied to location reference data.
2. **Layer Processing**: For each attribute layer being joined, the system runs **Range Splitting** followed by **JSON Unnesting** to flatten complex data types before they are merged.
3. **Scaling**: If specific scaling factors are defined in the configuration, **Numeric Scaling** is applied to the base geographic layers.
4. **Final Classification**: At the very end of the pipeline, once all layers are joined, **Temporal Column Classification** categorizes all columns into temporal buckets before writing to the final output formats.

```{mermaid}
flowchart TD
    Raw[Raw Data from Storage/API]
    RD(rename_and_drop)
    CL(clean_label / clean_tehsils)
    SC(split_cols)
    UJ(unnest_json_cols)
    AS(apply_scaling)
    CC(classify_columns)
    Out[Final Exported Data]

    Raw --> RD
    RD --> CL
    CL --> SC
    SC --> UJ
    UJ --> AS
    AS --> CC
    CC --> Out
```

## 1. Label Normalization (`clean_label`, `clean_tehsils`)
Location labels are normalized into safe, lowercase strings.
* **Process**: Any run of non-alphanumeric characters is collapsed to a single underscore (`_`), and leading/trailing underscores are stripped.
* **Example**: `"Raipur District"` becomes `"raipur_district"`, and `"North-East Delhi"` becomes `"north_east_delhi"`.

## 2. Column Renaming and Dropping (`rename_and_drop`)
Columns can be selectively renamed or dropped based on a configuration schema.
* **Process**: The system supports **glob patterns** (e.g., `k_*` mapping to `kharif_*`). This allows for dynamic renaming without hardcoding every single column name.
* Exact string matches are executed case-insensitively.

## 3. Numeric Scaling (`apply_scaling`)
Data columns can be mathematically scaled (multiplied by a specific factor).
* **Process**: Similar to renaming, column names are matched via glob patterns. Matching columns are cast to `Float64` and multiplied by the scaling factor.

## 4. Range Splitting (`split_cols`)
Some raw data might contain range strings instead of single numeric values.
* **Process**: The system detects columns containing strings like `"30 - 200"` or `"2160 to 4752"`.
* It automatically drops the original string column and generates two new numeric columns:
  * `{column_name}_min`
  * `{column_name}_max`
* It handles edge cases like word prefixes (e.g., `"upto 50"`) and strips unwanted characters like spaces and percentage signs (`%`).

## 5. JSON Unnesting (`unnest_json_cols`)
Some data columns contain serialized JSON dictionaries.
* **Process**: The system parses columns that fit specific prefix-and-year regex patterns (e.g., `dw_2019_2020`).
* It decodes the JSON strings and extracts their keys into individual `Float64` or `String` columns.
* **Example**: `dw_2019_2020` containing `{"DeltaG": 10.0}` becomes `dw_deltag_2019_2020`.

## 6. Temporal Column Classification (`classify_columns`)
When data frames are merged, the columns are grouped into temporal buckets so they can be structured appropriately.
* **Static**: Core identity columns (e.g., `mws_id`, `geometry`, `state`) that are preserved across time.
* **Fortnightly**: Columns ending with an ISO date (`YYYY-MM-DD`).
* **Annual**: Columns containing a single year or a year range (e.g., `2019_2020` or `2023`).
* **Dropped**: Columns containing the substring `"net"` are considered derived data and are dropped entirely during classification.
