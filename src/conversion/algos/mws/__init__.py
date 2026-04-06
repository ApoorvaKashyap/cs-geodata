import asyncio
import json

import polars as pl

from src.app.models import LayerConversionRequest
from src.conversion.helpers.api import get_active
from src.conversion.helpers.cleaners import clean_tehsils

_mws_layers_version: dict = {}


async def run_mws_pipeline(request: LayerConversionRequest) -> None:
    _ = _fetch_version(request.layer_version)
    _ = clean_tehsils(await get_active())


def _fetch_version(s3_path: str) -> pl.DataFrame:
    return pl.read_csv(s3_path).sort("State")


if __name__ == "__main__":
    with open("examples/mws.json") as f:
        request = json.load(f)
    request = LayerConversionRequest(**request)
    asyncio.run(run_mws_pipeline(request))
