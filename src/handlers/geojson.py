from ..utils.downloader import Downloader
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
            # 1. Use the FULL PATH to read the file
            json_path = Path(self._downloader.file)
            output_folder = Path("/tmp/parquet")
            output_folder.mkdir(parents=True, exist_ok=True)

            # Construct the parquet path directly using the ID
            parquet_path = output_folder / f"{self._id}.parquet"

            print(f"Converting: {json_path.name} -> {parquet_path.name}")

            # Load and convert only the specific file
            gdf = gpd.read_file(json_path)

            if not gdf.empty:
                if gdf.crs is None:
                    gdf.set_crs(epsg=4326, inplace=True)

                gdf.to_parquet(parquet_path, compression="snappy")
                print("Conversion Complete.")

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
