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
