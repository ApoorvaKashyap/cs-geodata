import json
import logging
import re

import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column classification regexes
# ---------------------------------------------------------------------------
# Matches a column that ends with an ISO date (YYYY-MM-DD), with or without
# a preceding underscore — e.g. "dg_deltag_2023-04-01" or "2023-04-01".
_FORTNIGHTLY_DATE_RE = re.compile(r"\d{1,4}-\d{1,2}-\d{1,4}$")
# Matches a year-range (YYYY_YYYY / YYYY-YYYY) or a bare year anywhere in the
# column name — e.g. "ci_kharif_2019_2020" or "te_slope_2023".
_ANNUAL_YEAR_RE = re.compile(r"(\d{4}[_-]\d{4}|\d{4})$")


def clean_label(label: str) -> str:
    """Normalise a location label to a safe, lowercase slug.

    Collapses any run of non-alphanumeric characters to a single underscore
    and strips leading/trailing underscores.

    Args:
        label: The original label string.

    Returns:
        The cleaned label string.

    Examples::

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


def expand_rename_globs(cols: list[str], rename_dict: dict[str, str]) -> dict[str, str]:
    """Expand a dictionary with glob patterns into explicit column renames.

    E.g., {"k_*": "kharif_*"} applied to ["k_2018", "k_2019"]
    returns {"k_2018": "kharif_2018", "k_2019": "kharif_2019"}.
    Non-glob exact matches are kept as is, but are matched case-insensitively.
    """
    expanded = {}
    col_lower_map = {c.lower(): c for c in cols}

    for k, v in rename_dict.items():
        k_lower = k.lower()
        if "*" in k_lower:
            parts = k_lower.split("*")
            if len(parts) == 2:
                prefix, suffix = parts
                pattern = re.compile(
                    f"^{re.escape(prefix)}(.*){re.escape(suffix)}$", re.IGNORECASE
                )
                for col in cols:
                    m = pattern.match(col)
                    if m:
                        captured = m.group(1)
                        new_col = v.replace("*", captured) if "*" in v else v
                        expanded[col] = new_col
        else:
            if k_lower in col_lower_map:
                expanded[col_lower_map[k_lower]] = v
            else:
                expanded[k] = v
    return expanded


def expand_drop_globs(cols: list[str], drop_list: list[str]) -> list[str]:
    """Expand a list of column names with optional glob patterns.

    E.g. ["prefix_*", "exact_name"] -> ["prefix_1", "prefix_2", "exact_name"].
    """
    import fnmatch

    expanded = set()
    col_lower_map = {c.lower(): c for c in cols}

    for pattern in drop_list:
        pattern_lower = pattern.lower()
        if "*" in pattern_lower or "?" in pattern_lower:
            for c in cols:
                if fnmatch.fnmatch(c.lower(), pattern_lower):
                    expanded.add(c)
        else:
            # For exact matches, prefer the actual casing in the dataframe if it exists
            if pattern_lower in col_lower_map:
                expanded.add(col_lower_map[pattern_lower])
            else:
                expanded.add(pattern)
    return list(expanded)


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
    cols = layer.collect_schema().names()
    expanded_rename = expand_rename_globs(cols, rename)
    expanded_drop = expand_drop_globs(cols, drop)

    return (
        layer.drop(expanded_drop, strict=False)
        .rename(expanded_rename, strict=False)
        .select(pl.all().name.to_lowercase())
    )


def apply_scaling(layer: pl.LazyFrame, scale_dict: dict[str, float]) -> pl.LazyFrame:
    """Multiply matching columns by the specified factor.

    Supports glob patterns in scale_dict keys.
    """
    if not scale_dict:
        return layer

    schema_cols = layer.collect_schema().names()
    col_to_factor = {}

    for pattern, factor in scale_dict.items():
        # Use case-insensitive matching just in case
        for c in schema_cols:
            import fnmatch

            if fnmatch.fnmatch(c.lower(), pattern.lower()):
                col_to_factor[c] = factor

    if col_to_factor:
        from loguru import logger

        logger.info(
            f"apply_scaling: successfully applying scale factors: {col_to_factor}"
        )
        exprs = [
            (pl.col(c).cast(pl.Float64, strict=False) * factor).alias(c)
            for c, factor in col_to_factor.items()
        ]
        return layer.with_columns(exprs)

    return layer


def get_layer_prefix(layer: str) -> str:
    """Derive a short 2-character prefix for each layer name.

    Rules (in order):

    - ``"annual_balance"`` → first char of each ``"_"``-separated part → ``"ab_"``
    - ``"water-balance"``  → first char of each ``"-"``-separated part → ``"wb_"``
    - ``"aquifer"``        → first two chars + ``"_"``                 → ``"aq_"``

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
    # The prefix group *must* start with a letter so that a raw year token like
    # "2017_" in "2017_2018" is never consumed as the prefix — only named
    # prefixes such as "dw_" or "ci_" qualify as group(1).
    pattern = re.compile(
        r"^([a-zA-Z][a-zA-Z0-9]*(?:_[a-zA-Z][a-zA-Z0-9]*)*_)?(\d{4}(?:[-_]\d{2,4}){0,2})$"
    )

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
            except Exception as exc:  # noqa: BLE001
                logger.debug("Could not sniff JSON schema from column %s: %s", c, exc)
                continue

    if not keys or not dtype:
        return layer

    exprs = []

    for c in json_cols:
        parsed = pl.col(c).str.json_decode(dtype)
        match = pattern.match(c)
        if match is None:  # json_cols only contains columns that matched pattern
            raise RuntimeError(f"Pattern unexpectedly did not match column: {c!r}")
        prefix = match.group(1) or ""
        suffix = match.group(2)

        for k in keys:
            new_col = f"{prefix}{k.lower()}_{suffix}"
            exprs.append(parsed.struct.field(k).alias(new_col))

    return layer.with_columns(exprs).drop(json_cols)


def classify_columns(
    cols: list[str],
    keep_always: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """Classify merged-frame column names into static, fortnightly, and annual buckets.

    Classification rules (applied in order):

    1. Columns in *keep_always* → static (identity / common columns).
    2. Columns whose name contains the substring ``"net"`` → dropped entirely
       (they hold derived data that is not needed downstream).
    3. Columns whose name ends with an ISO date (``YYYY-MM-DD``) → fortnightly.
    4. Columns whose name contains a year-range (``YYYY_YYYY`` / ``YYYY-YYYY``)
       or a bare four-digit year anywhere → annual.
    5. Everything else → static.

    Args:
        cols: All column names from the merged frame.
        keep_always: Column names that must always land in the static bucket
            (e.g. ``["mws_id", "geometry", "tehsil", ...]``).

    Returns:
        A 3-tuple ``(static_cols, fortnightly_cols, annual_cols)`` where every
        column in *cols* appears in exactly one bucket, or is silently dropped
        (net columns).
    """
    keep_set = set(keep_always)
    static: list[str] = list(keep_always)  # preserve order of common cols first
    fortnightly: list[str] = []
    annual: list[str] = []

    for col in cols:
        if col in keep_set:
            continue  # already added above

        # Drop derived net columns
        if "net" in col:
            continue

        if _FORTNIGHTLY_DATE_RE.search(col):
            fortnightly.append(col)
        elif _ANNUAL_YEAR_RE.search(col):
            annual.append(col)
        else:
            static.append(col)

    return static, fortnightly, annual
