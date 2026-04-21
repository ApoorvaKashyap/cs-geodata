import json
import re

import polars as pl


def clean_label(label: str) -> str:
    """Normalise a location label to a safe, lowercase slug.

    Collapses any run of non-alphanumeric characters to a single underscore
    and strips leading/trailing underscores.

    Args:
        label: The original label string.

    Returns:
        The cleaned label string.

    Examples:
        "Raipur District"  -> "raipur_district"
        "North-East Delhi" -> "north_east_delhi"
        "  Pune  "         -> "pune"
    """
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def clean_tehsils(response: pl.DataFrame) -> pl.LazyFrame:
    """Extract and normalize tehsils from the active areas API response.

    Args:
        response: The raw DataFrame containing nested district/tehsil data.

    Returns:
        A LazyFrame exploded to the tehsil level with state_name, district_name,
        and tehsil_name.
    """
    df_districts = response.explode("district").with_columns(
        [
            pl.col("district").struct.field("label").alias("district_name"),
            pl.col("district").struct.field("district_id").alias("district_id"),
            pl.col("district").struct.field("blocks").alias("tehsil"),  # still a list
        ]
    )

    # Further explode tehsils
    df_tehsils = (
        df_districts.explode("tehsil")
        .with_columns(
            [
                pl.col("tehsil").struct.field("label").alias("tehsil_name"),
                pl.col("tehsil").struct.field("tehsil_id").alias("tehsil_id"),
            ]
        )
        .drop("district", "tehsil")
    )
    df_tehsils = df_tehsils.rename({"label": "state_name"})

    return df_tehsils.lazy()


def rename_and_drop(
    layer: pl.LazyFrame, rename: dict[str, str], drop: list[str]
) -> pl.LazyFrame:
    """Rename columns and drop specified columns from a layer.

    Args:
        layer: The layer LazyFrame to process.
        rename: Dictionary mapping original names to target names.
        drop: List of column names to drop.

    Returns:
        The processed LazyFrame with lowercased column names.
    """
    return (
        layer.drop(drop, strict=False)
        .rename(rename, strict=False)
        .select(pl.all().name.to_lowercase())
    )


def get_layer_prefix(layer: str) -> str:
    """Derive a short 2-character prefix for each layer name.

    Rules (in order):
        "annual_balance"  -> first char of each part joined by "_"  -> "ab_"
        "water-balance"   -> first char of each part joined by "-"  -> "wb_"
        "aquifer"         -> first two chars + "_"                  -> "aq_"

    Args:
        layer: The full layer name.

    Returns:
        A 2-3 character string prefix.
    """
    if "_" in layer:
        parts = layer.split("_")
        return parts[0][0] + parts[1][0] + "_"
    elif "-" in layer:
        parts = layer.split("-")
        return parts[0][0] + parts[1][0] + "_"
    else:
        return layer[:2] + "_"


def prefix_cols(
    layer: pl.LazyFrame, layer_name: str, common_cols: list[str]
) -> pl.LazyFrame:
    """Prefix non-common columns with the derived layer prefix.

    Args:
        layer: The layer LazyFrame.
        layer_name: Name of the layer used to derive the prefix.
        common_cols: List of common column names to exclude from prefixing.

    Returns:
        The LazyFrame with specific columns prefixed.
    """
    common_cols = [c.lower() for c in common_cols]
    layer_cols = [c.lower() for c in layer.collect_schema().names()]
    existing_common = [c for c in common_cols if c in layer_cols]

    if not existing_common:
        return layer.select([pl.all().name.prefix(get_layer_prefix(layer_name))])

    return layer.select(
        [
            pl.exclude(existing_common).name.prefix(get_layer_prefix(layer_name)),
            pl.col(existing_common),
        ]
    )


def merge_col_metadata(version: pl.LazyFrame, tehsils: pl.LazyFrame) -> pl.LazyFrame:
    """Join layer version metadata with active tehsil information.

    Args:
        version: LazyFrame containing version metadata.
        tehsils: LazyFrame containing tehsil information.

    Returns:
        A merged LazyFrame combining version metadata with tehsil identities.
    """
    version = version.rename(lambda c: c.lower())
    tehsils = tehsils.rename(lambda c: c.lower())
    merged = tehsils.join(
        version,
        left_on=["state_name", "district_name", "tehsil_name"],
        right_on=["state", "district", "tehsil"],
        how="left",
    )
    return merged


def split_cols(layer: pl.LazyFrame) -> pl.LazyFrame:
    """Split range-based string columns into min/max numeric columns.

    Detects columns containing ranges (e.g. "30 - 200", "2160 to 4752") and
    generates two new columns (`col_min`, `col_max`) for each matching column,
    dropping the original string column.

    Args:
        layer: The layer LazyFrame to process.

    Returns:
        The LazyFrame with range columns split into min/max bounds.
    """
    # Optional word prefix like "upto ", "up to "
    WORD_PREFIX = r"(?:[A-Za-z]+\s*)+"
    # Core numeric pattern: optional word prefix, number, optional range/unit suffix
    regex = (
        r"^\s*(?:" + WORD_PREFIX + r")?"
        r"\d+(\.\d+)?"
        r"\s*(?:%|(?:\s*(?:-|to)\s*\d+(\.\d+)?\s*%?))?"
        r"\s*$"
    )
    # detect needs at least one range/unit marker to avoid matching pure numeric cols
    regex_detect = (
        r"^\s*(?:" + WORD_PREFIX + r")?"
        r"\d+(\.\d+)?"
        r"\s*(?:%|(?:\s*(?:-|to)\s*\d+(\.\d+)?\s*%?))"
        r"\s*$"
    )

    cols = [
        name for name, dtype in layer.collect_schema().items() if dtype == pl.String
    ]

    check_results = layer.select(
        [
            (pl.col(c).is_not_null() & pl.col(c).str.contains(regex_detect))
            .sum()
            .alias(c)
            for c in cols
        ]
    ).collect(engine="streaming")

    ok_cols = [c for c in cols if check_results[c][0] > 0]

    derived_exprs = []
    for c in ok_cols:
        clean_col = (
            pl.col(c)
            .str.replace(r"(?i)^\s*(?:[A-Za-z]+\s*)+", "")  # strip word prefix
            .str.replace_all(r"%", "")  # strip %
            .str.replace_all(
                r"\s+", ""
            )  # strip ALL spaces (handles "30 -200", "2160 to4752")
            .str.replace(r"(?i)to", "-")  # normalise "to" -> "-"
            .str.split("-")  # split on "-"
        )

        derived_exprs.append(
            pl.when(pl.col(c).is_in(["-", None]) | ~pl.col(c).str.contains(regex))
            .then(None)
            .otherwise(
                clean_col.list.get(0, null_on_oob=True).cast(pl.Float64, strict=False)
            )
            .alias(f"{c}_min")
        )

        derived_exprs.append(
            pl.when(pl.col(c).is_in(["-", None]) | ~pl.col(c).str.contains(regex))
            .then(None)
            .otherwise(
                pl.coalesce(
                    [
                        clean_col.list.get(1, null_on_oob=True),
                        clean_col.list.get(0, null_on_oob=True),
                    ]
                ).cast(pl.Float64, strict=False)
            )
            .alias(f"{c}_max")
        )

    layer = layer.with_columns(derived_exprs).drop(ok_cols)
    return layer


def unnest_json_cols(layer: pl.LazyFrame) -> pl.LazyFrame:
    """Unnest columns containing JSON dicts into separate columns.

    E.g. dw_2019_2020 containing {"DeltaG": 10.0} becomes dw_deltag_2019_2020

    Args:
        layer: The layer LazyFrame to process.

    Returns:
        The LazyFrame with unnested JSON attributes as separate columns.
    """
    schema = layer.collect_schema()
    pattern = re.compile(r"^(.*?_)?(\d{4}(?:[-_]\d{2,4}){0,2})$")

    json_cols = [
        c for c, dtype in schema.items() if dtype == pl.String and pattern.match(c)
    ]

    if not json_cols:
        return layer

    # Attempt to sniff schema keys and types from the first non-null JSON body
    keys = None
    dtype = None
    for c in json_cols:
        sample = (
            layer.select(pl.col(c).drop_nulls()).head(1).collect(engine="streaming")
        )
        if not sample.is_empty():
            try:
                sample_str = sample[0, 0]
                sample_json = json.loads(sample_str)
                keys = list(sample_json.keys())

                fields = []
                for k, v in sample_json.items():
                    # If it's a number, treat as Float64. Otherwise String.
                    if isinstance(v, (int, float)):
                        fields.append(pl.Field(k, pl.Float64))
                    else:
                        fields.append(pl.Field(k, pl.String))
                dtype = pl.Struct(fields)
                break
            except Exception:
                continue

    if not keys or not dtype:
        return layer

    exprs = []

    for c in json_cols:
        parsed = pl.col(c).str.json_decode(dtype)
        match = pattern.match(c)
        assert match is not None  # json_cols only contains columns that matched pattern
        prefix = match.group(1) or ""
        suffix = match.group(2)

        for k in keys:
            new_col = f"{prefix}{k.lower()}_{suffix}"
            exprs.append(parsed.struct.field(k).alias(new_col))

    return layer.with_columns(exprs).drop(json_cols)
