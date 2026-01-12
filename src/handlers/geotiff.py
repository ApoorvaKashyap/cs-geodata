"""GeoTIFF handler module for downloading, processing, and converting GeoTIFF files.

This module provides the GeoTiffHandler class for managing GeoTIFF files from
remote sources. It handles downloading GeoTIFF files, loading them using rioxarray,
and converting them to Zarr format for efficient storage and access.
"""

import rioxarray as rxr
import xarray as xr

from ..utils.downloader import Downloader


class GeoTiffHandler:
    _dataset = None

    def __init__(self, id: str, url: str, path: str = "/tmp/geotiff") -> None:
        self._id = id
        self._url = url
        self._path = path
        self._downloader = Downloader(self._url, f"{self._path}/{self._id}.tif")
        xr.set_options(keep_attrs=True)

    async def _download(self) -> int:
        """Download the GeoTIFF file from the remote URL.

        Attempts to download using asyncstart(), and falls back to direct
        download() if a ValueError occurs. Ensures proper cleanup of aiohttp
        sessions.

        Returns:
            1 if download succeeds, -1 if an error occurs.

        Note:
            Prints error messages to stdout when exceptions occur.
        """
        try:
            await self._downloader.asyncstart()
            return 1
        except ValueError as _:
            await self._downloader.download()
            if self._downloader.new_session:
                await self._downloader.session.close()
            return 1
        except Exception as e:
            print(f"Error downloading GeoTIFF file: {e}")
            if self._downloader.new_session and not self._downloader.session.closed:
                await self._downloader.session.close()
            return -1

    async def _load(self) -> int:
        """Load the downloaded GeoTIFF file into memory.

        Opens the GeoTIFF file using rioxarray and prints metadata including
        name, dimensions, and bounding box.

        Returns:
            1 if loading succeeds, -1 if an error occurs.

        Note:
            Sets self._dataset to the loaded xarray Dataset.
            Prints file metadata and error messages to stdout.
        """
        try:
            self._dataset = rxr.open_rasterio(f"{self._path}/{self._id}.tif")
            print(
                f"Name: {self._dataset.name}\nDimensions: {self._dataset.dims}\
                    \nBounding Box: {self._dataset.rio.bounds()}"
            )
            return 1
        except Exception as e:
            print(f"Error loading GeoTIFF file: {e}")
            return -1

    async def _save(self) -> int:
        """Save the loaded dataset to Zarr format.

        Converts the in-memory xarray Dataset to Zarr format for efficient
        storage and chunked access.

        Returns:
            1 if saving succeeds, -1 if an error occurs.

        Note:
            Requires that _load() has been called successfully first.
            Prints error messages to stdout when exceptions occur.
        """
        try:
            self._dataset.to_zarr(f"{self._path}/{self._id}.zarr")
            return 1
        except Exception as e:
            print(f"Error saving GeoTIFF file: {e}")
            return -1

    async def handle(self) -> int:
        """Execute the complete GeoTIFF processing workflow.

        Orchestrates the full workflow:
        1. Downloads the GeoTIFF file from the remote URL
        2. Loads the file into memory using rioxarray
        3. Converts and saves to Zarr format

        Returns:
            1 if all steps succeed, -1 if any step fails.

        Note:
            Prints progress messages to stdout at each step.
            Returns immediately if any step fails.
        """
        print("Downloading GeoTIFF file...")
        status = await self._download()
        if status != 1:
            print("Failed to download GeoTIFF file")
            return -1
        print("GeoTIFF file downloaded successfully")
        print("Loading GeoTIFF file...")
        status = await self._load()
        if status != 1:
            print("Failed to load GeoTIFF file")
            return -1
        print("GeoTIFF file loaded successfully")
        print("Saving GeoTIFF file...")
        status = await self._save()
        if status != 1:
            print("Failed to save GeoTIFF file")
            return -1
        print("GeoTIFF file saved successfully")
        return status
