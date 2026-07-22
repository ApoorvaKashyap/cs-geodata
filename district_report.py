import argparse
import pandas as pd
import os
import duckdb
import json
import glob
from collections import defaultdict


def analyze_district(district, base_dir, out_dir):
    safe_dist = (
        str(district).strip().replace("/", "_").replace(" ", "_").replace(",", "")
    )
    if not safe_dist:
        return

    print(f"Processing district: {district}")

    con = duckdb.connect()

    # Check if district exists
    found_any = False
    for cat in ["static", "annual", "sub-annual"]:
        if not glob.glob(f"{base_dir}/{cat}/**/*.parquet", recursive=True):
            continue
        try:
            q = f"SELECT count(*) FROM read_parquet('{base_dir}/{cat}/**/*.parquet', union_by_name=true) WHERE list_contains(district, '{district.replace("'", "''")}')"
            if con.execute(q).fetchone()[0] > 0:
                found_any = True
                break
        except Exception:
            pass

    if not found_any:
        print(f"  -> Error: District '{district}' not found. Skipping.")
        con.close()
        return

    report = {
        "district": district.strip(),
        "categories": defaultdict(
            lambda: {
                "null_percentages": {},
                "completely_null_columns": [],
                "most_empty_tehsils": {},
            }
        ),
    }

    for cat in ["static", "annual", "sub-annual"]:
        if not glob.glob(f"{base_dir}/{cat}/**/*.parquet", recursive=True):
            continue

        query = f"""
            SELECT * FROM read_parquet('{base_dir}/{cat}/**/*.parquet', union_by_name=true)
            WHERE list_contains(district, '{district.replace("'", "''")}')
        """
        try:
            df = con.execute(query).df()
            if len(df) == 0:
                continue

            df_exploded = df.explode("tehsil") if "tehsil" in df.columns else df

            # Identify data columns
            exclude = ["district", "tehsil", "state", "geometry", "id", "key"]
            data_cols = [c for c in df.columns if c not in exclude]

            total_rows = len(df)
            cat_dict = report["categories"][cat]

            # Column null percentages
            for c in data_cols:
                nulls = int(df[c].isna().sum())
                pct = float((nulls / total_rows) * 100) if total_rows > 0 else 100.0
                cat_dict["null_percentages"][c] = round(pct, 2)
                if pct == 100.0:
                    cat_dict["completely_null_columns"].append(c)

            # Tehsil missingness
            if "tehsil" in df_exploded.columns and len(data_cols) > 0:
                missing_by_tehsil = (
                    df_exploded[data_cols]
                    .isna()
                    .sum(axis=1)
                    .groupby(df_exploded["tehsil"])
                    .sum()
                )
                total_by_tehsil = df_exploded.groupby("tehsil").size() * len(data_cols)
                pct_series = (missing_by_tehsil / total_by_tehsil) * 100

                # Top 3 most empty
                top_empty = pct_series.sort_values(ascending=False).head(3)

                empty_dict = {}
                for t, v in top_empty.items():
                    if pd.notna(v) and t is not None:
                        empty_dict[str(t)] = round(float(v), 2)

                cat_dict["most_empty_tehsils"] = empty_dict

        except Exception as e:
            print(f"  -> Error processing {cat} for {district}: {e}")

    con.close()

    # Convert defaultdict to standard dict for JSON serialization
    report["categories"] = dict(report["categories"])

    out_file = os.path.join(out_dir, f"{safe_dist}_report.json")
    with open(out_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"  -> Saved report to {out_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate district data completeness report in JSON."
    )
    parser.add_argument(
        "districts_file", type=str, help="File containing one district name per line"
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="/home/akashyap54/workstation/CoREStack-Data/mws",
        help="Base directory of raw data",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="/home/akashyap54/Projects/CoreStack/core-lens/district_reports",
        help="Output directory",
    )
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    if not os.path.exists(args.districts_file):
        print(f"Error: Input file {args.districts_file} does not exist.")
        return

    with open(args.districts_file, "r") as f:
        districts = [line.strip() for line in f if line.strip()]

    print(f"Found {len(districts)} districts to process.")

    for d in districts:
        analyze_district(d, args.base_dir, args.out_dir)


if __name__ == "__main__":
    main()
