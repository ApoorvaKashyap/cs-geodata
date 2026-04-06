import polars as pl
import requests

from src.utils.configs import settings


async def get_active() -> pl.DataFrame:
    response = requests.get(
        f"{settings.corestack_api_url}/get_active_locations/",
        headers={"X-API-KEY": f"{settings.corestack_api_key.get_secret_value()}"},
    )
    _df = pl.read_json(response.content)
    return _df


async def get_geojson(layer: str, district: str, tehsil: str) -> int:
    # Sample URL: https://geoserver.core-stack.org:8443/geoserver/aquifer/ows?service=WFS&version=1.0.0&request=GetFeature
    # &typeName=aquifer%3Aaquifer_vector_banka_banka&outputFormat=application%2Fjson
    url = (
        f"{settings.base_geoserver}/{layer}/ows?service=WFS&version=2.0.0&request=GetFeature"
        f"&typeName={layer.lower()}%3A{layer.lower()}_vector_{district.lower()}_{tehsil.lower()}&outputFormat=application%2Fjson"
    )
    response = requests.get(url)
    if response.status_code == 200:
        try:
            with open(
                f"{settings.temp_path}/{layer}_{district}_{tehsil}.geojson", "w"
            ) as f:
                f.write(response.text)
            return 0
        except Exception as e:
            print(e)
    return -1
