import re

import polars as pl


def clean_label(label: str) -> str:
    """
    Normalise a location label to a safe, lowercase slug.
    Collapses any run of non-alphanumeric characters to a single underscore
    and strips leading/trailing underscores.

    Examples
    --------
    "Raipur District"  -> "raipur_district"
    "North-East Delhi" -> "north_east_delhi"
    "  Pune  "         -> "pune"
    """
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def clean_tehsils(response: pl.DataFrame) -> pl.LazyFrame:
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

    return df_tehsils.lazy()
