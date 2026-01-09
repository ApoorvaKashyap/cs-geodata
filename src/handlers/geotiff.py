from ..utils.downloader import Downloader


class GeoTiffHandler:
    def __init__(self, id: str, url: str, temp_path: str = "/tmp/geotiff") -> None:
        self._id = id
        self._url = url
        print(self._url)
        self._downloader = Downloader(self._url, f"{temp_path}/{self._id}.tif")

    async def handle(self) -> int:
        print("Downloading GeoTIFF file...")
        try:
            await self._downloader.asyncstart()
        except ValueError as _:
            await self._downloader.download()
        except Exception as e:
            print(f"Error downloading GeoTIFF file: {e}")
            return -1
        return 1
