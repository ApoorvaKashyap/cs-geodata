from ..utils.downloader import Downloader


class GeoJSONHandler:
    def __init__(self, id: str, url: str, temp_path: str = "/tmp/geojson") -> None:
        self._id = id
        self._url = url
        self._downloader = Downloader(self._url, f"{temp_path}/{self._id}.geojson")

    async def handle(self) -> int:
        print("Downloading GeoJSON file...")
        try:
            await self._downloader.asyncstart()
        except ValueError as _:
            await self._downloader.download()
        except Exception as e:
            print(f"Error downloading GeoJSON file: {e}")
            return -1
        return 1
