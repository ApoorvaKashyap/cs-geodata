"""Pipeline write stage — GeoParquet (static) and Parquet (temporal) output."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import fsspec
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import shapely.wkb

from src.descriptor.schema import EntityDescriptor, LayerDescriptor, LayerResolution
from src.stac.client import StacSourceInfo
from src.utils.duckdb import duckdb_connection
from src.utils.s3 import get_s3_storage_options


def write_layer(
    frame: pl.LazyFrame,
    layer: LayerDescriptor,
    descriptor: EntityDescriptor,
    source_info: StacSourceInfo,
    output_root: str,
) -> None:
    """Write a reshaped LazyFrame to the ecolib output directory tree.

    For static layers, writes a partitioned GeoParquet file via DuckDB
    ``COPY … PARTITION BY`` with CRS applied, then patches GeoParquet 1.1
    metadata (global bbox, ``covering`` declaration) using pyarrow.

    For temporal layers, writes a plain Parquet file directly with Polars.

    Output paths follow the ecolib layout::

        {output_root}/{entity}/static.geoparquet
        {output_root}/{entity}/annual.parquet
        {output_root}/{entity}/fortnightly.parquet

    Args:
        frame: Reshaped LazyFrame ready for output.
        layer: Layer Descriptor declaring resolution and geometry/partition info.
        descriptor: Entity Descriptor carrying entity name and partition column.
        source_info: Resolved STAC source metadata (CRS, etc.).
        output_root: Root path or URI for ecolib output data.
    """
    write_resolution(
        frame,
        layer.resolution,
        descriptor,
        source_info,
        output_root,
    )


def write_resolution(
    frame: pl.LazyFrame,
    resolution: LayerResolution,
    descriptor: EntityDescriptor,
    source_info: StacSourceInfo,
    output_root: str,
) -> None:
    """Write one merged output frame for an entity resolution.

    Args:
        frame: Merged LazyFrame ready for output.
        resolution: Output resolution to write.
        descriptor: Entity Descriptor carrying entity name and partition column.
        source_info: Resolved STAC source metadata used for CRS.
        output_root: Root path or URI for ecolib output data.
    """
    if _is_s3_uri(output_root):
        with TemporaryDirectory() as tmpdir:
            local_root = Path(tmpdir)
            _write_resolution_local(
                frame,
                resolution,
                descriptor,
                source_info,
                local_root,
            )
            _upload_entity_output(
                local_root / descriptor.entity,
                _join_uri(output_root, descriptor.entity),
            )
        return

    _write_resolution_local(
        frame, resolution, descriptor, source_info, Path(output_root)
    )


def _write_resolution_local(
    frame: pl.LazyFrame,
    resolution: LayerResolution,
    descriptor: EntityDescriptor,
    source_info: StacSourceInfo,
    output_root: Path,
) -> None:
    entity_dir = Path(output_root) / descriptor.entity
    entity_dir.mkdir(parents=True, exist_ok=True)

    if resolution == "static":
        _write_geoparquet(frame, descriptor, source_info, entity_dir)
    else:
        _write_temporal_parquet(frame, resolution, entity_dir)


def _upload_entity_output(local_entity_dir: Path, remote_entity_uri: str) -> None:
    storage_options = get_s3_storage_options()
    fs, remote_path = fsspec.core.url_to_fs(remote_entity_uri, **storage_options)
    fs.makedirs(remote_path, exist_ok=True)

    for local_file in local_entity_dir.rglob("*"):
        if not local_file.is_file():
            continue

        relative_path = local_file.relative_to(local_entity_dir).as_posix()
        remote_file = f"{remote_path.rstrip('/')}/{relative_path}"
        parent = remote_file.rsplit("/", 1)[0]
        fs.makedirs(parent, exist_ok=True)
        with local_file.open("rb") as src, fs.open(remote_file, "wb") as dst:
            dst.write(src.read())


def _is_s3_uri(uri: str) -> bool:
    return uri.startswith("s3://")


def _join_uri(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *[part.strip("/") for part in parts]])


def _write_geoparquet(
    frame: pl.LazyFrame,
    descriptor: EntityDescriptor,
    source_info: StacSourceInfo,
    entity_dir: Path,
) -> None:
    output_path = entity_dir / "static.geoparquet"
    crs_code = source_info.crs or "EPSG:4326"

    collected = frame.collect()

    with duckdb_connection() as conn:
        conn.register("source_view", collected.to_arrow())

        geometry_col = descriptor.geometry
        partition_col = descriptor.partition_by

        conn.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE ({geometry_col}),
                    ST_SetSRID(ST_GeomFromGeoJSON({geometry_col}), {_parse_epsg(crs_code)})
                        AS {geometry_col}
                FROM source_view
            )
            TO '{output_path}'
            (
                FORMAT PARQUET,
                COMPRESSION SNAPPY,
                PARTITION_BY ({partition_col}),
                OVERWRITE_OR_IGNORE
            )
        """)

    _patch_geoparquet_metadata(output_path, geometry_col, crs_code)


def _write_temporal_parquet(
    frame: pl.LazyFrame,
    resolution: LayerResolution,
    entity_dir: Path,
) -> None:
    filename = f"{resolution}.parquet"
    output_path = entity_dir / filename
    frame.sink_parquet(output_path, compression="snappy")


def _patch_geoparquet_metadata(
    geoparquet_path: Path,
    geometry_col: str,
    crs_code: str,
) -> None:
    partition_dirs = [p for p in geoparquet_path.rglob("*.parquet") if p.is_file()]
    if not partition_dirs:
        partition_dirs = [geoparquet_path] if geoparquet_path.is_file() else []

    all_bboxes: list[tuple[float, float, float, float]] = []

    for part_file in partition_dirs:
        schema = pq.read_schema(part_file)
        existing_meta = schema.metadata or {}
        geo_meta = _build_geo_metadata(geometry_col, crs_code)

        updated_schema = schema.with_metadata(
            {**existing_meta, b"geo": json.dumps(geo_meta).encode()}
        )

        table = pq.read_table(part_file)
        pq.write_table(table.cast(updated_schema), part_file, compression="snappy")

        bbox = _compute_bbox(table, geometry_col)
        if bbox:
            all_bboxes.append(bbox)

    if all_bboxes and geoparquet_path.is_dir():
        _write_global_metadata(geoparquet_path, geometry_col, crs_code, all_bboxes)


def _build_geo_metadata(geometry_col: str, crs_code: str) -> dict[str, object]:
    return {
        "version": "1.1.0",
        "primary_column": geometry_col,
        "columns": {
            geometry_col: {
                "encoding": "WKB",
                "geometry_types": [],
                "crs": crs_code,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }


def _compute_bbox(
    table: pa.Table,
    geometry_col: str,
) -> tuple[float, float, float, float] | None:
    if geometry_col not in table.schema.names:
        return None

    geoms = []
    for cell in table.column(geometry_col):
        if not cell.is_valid or cell.as_py() is None:
            continue
        try:
            geoms.append(shapely.wkb.loads(bytes(cell.as_py())))
        except (TypeError, ValueError, shapely.errors.GEOSException):
            continue

    if not geoms:
        return None

    bounds = shapely.union_all(geoms).bounds
    return (bounds[0], bounds[1], bounds[2], bounds[3])


def _write_global_metadata(
    directory: Path,
    geometry_col: str,
    crs_code: str,
    bboxes: list[tuple[float, float, float, float]],
) -> None:
    global_bbox = (
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    )

    meta_path = directory / "_common_metadata"
    geo_meta = _build_geo_metadata(geometry_col, crs_code)
    geo_meta["columns"][geometry_col]["bbox"] = list(global_bbox)  # type: ignore[index]

    empty_schema = pa.schema([], metadata={b"geo": json.dumps(geo_meta).encode()})
    pq.write_metadata(empty_schema, meta_path)


def _parse_epsg(crs_code: str) -> int:
    parts = crs_code.upper().split(":")
    try:
        return int(parts[-1])
    except ValueError:
        return 4326
