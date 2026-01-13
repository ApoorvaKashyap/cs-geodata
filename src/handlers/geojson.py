from ..utils.downloader import Downloader
import os
import geopandas as gpd
from pathlib import Path


class GeoJSONHandler:
    def __init__(self, id: str, url: str, temp_path: str = "/tmp/geojson") -> None:
        self._id = id
        self._url = url
        self._downloader = Downloader(self._url, f"{temp_path}/{self._id}.geojson")

    async def handle(self) -> int:
        print("Downloading GeoJSON file...")
        try:
            await self._downloader.asyncstart()
            source_folder = Path("/tmp/geojson")
            output_folder = Path("/tmp/parquet")
            output_folder.mkdir(parents=True, exist_ok=True)

            print(f"Scanning: {source_folder.resolve()}")

            for root, dirs, files in os.walk(str(source_folder)):
                for file in files:
                    json_path = os.path.join(root, file)

                    rel_path = os.path.relpath(json_path, source_folder)
                    safe_name = (
                        rel_path.replace(os.sep, "_")
                        .replace(".geojson", "")
                        .replace(".json", "")
                    )
                    parquet_path = output_folder / f"{safe_name}.parquet"

                    try:
                        gdf = gpd.read_file(json_path)

                        if not gdf.empty:
                            if gdf.crs is None:
                                gdf.set_crs(epsg=4326, inplace=True)

                            gdf.to_parquet(parquet_path, compression="snappy")
                            print(f"Converted: {file} -> {parquet_path.name}")
                        else:
                            print(f"Skipped empty geometry: {file}")

                    except Exception:
                        # If it's a STAC Catalog/Collection JSON, it won't have geometries and will fail here
                        print(f"Skipped non-spatial file {file}")

            print("\nProcess Complete.")

            return 1
        except ValueError as _:
            await self._downloader.download()
            if self._downloader.new_session:
                await self._downloader.session.close()
            return 1
        except Exception as e:
            print(f"Error downloading GeoJSON file: {e}")
            if self._downloader.new_session and not self._downloader.session.closed:
                await self._downloader.session.close()
            return -1
