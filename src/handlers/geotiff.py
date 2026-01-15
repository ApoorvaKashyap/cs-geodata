"""GeoTIFF handler module for downloading, processing, and converting GeoTIFF files.

This module provides the GeoTiffHandler class for managing GeoTIFF files from
remote sources. It handles downloading GeoTIFF files, loading them using rioxarray,
and converting them to Zarr format for efficient storage and access.
"""

from pathlib import Path

import rioxarray as rxr
import xarray as xr

from ..utils.downloader import Downloader


class GeoTiffHandler:
    """Handler for downloading, loading, and converting GeoTIFF files to Zarr format.

    This class manages the complete workflow for processing GeoTIFF files:
    1. Downloads GeoTIFF files from a URL using async multi-threaded downloads
    2. Loads the data using rioxarray for geospatial processing
    3. Converts and saves to Zarr format for efficient chunked access

    The class maintains the dataset in memory after loading and provides
    methods for each step of the workflow, which can be executed individually
    or together through the handle() method.

    Attributes:
        _dataset: The loaded xarray DataArray containing GeoTIFF data.
            Initially None, populated after calling _load().
        _id: Unique identifier for the GeoTIFF file, used in file naming.
        _url: Source URL from which to download the GeoTIFF file.
        _path: Local directory path where files will be stored.
        _downloader: Downloader instance for async file download operations.

    Example:
        >>> handler = GeoTiffHandler(
        ...     id="elevation-data",
        ...     url="https://example.com/elevation.tif",
        ...     path="/data/geotiff"
        ... )
        >>> status = await handler.handle()
        >>> print(f"Processing status: {status}")
        Processing status: 1
    """

    _dataset = None

    def __init__(self, id: str, url: str, path: str = "/tmp/geotiff") -> None:
        """Initialize the GeoTiffHandler.

        Args:
            id: Unique identifier for the GeoTIFF file, used in naming.
            url: URL from which to download the GeoTIFF file.
            path: Local directory path where files will be stored.
                Defaults to "/tmp/geotiff".
        """
        self._id: str = id
        self._url: str = url
        self._path: str = path
        self._downloader: Downloader = Downloader(
            self._url, f"{self._path}/{self._id}.tif"
        )
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
            if Path(f"{self._path}/{self._id}.tif").is_file():
                print("GeoTIFF file already exists! Skipping Download!")
                return 1
            await self._downloader.asyncstart()
            status = 1
        except ValueError as _:
            await self._downloader.download()
            status = 1
        except Exception as e:
            print(f"Error downloading GeoTIFF file: {e}")
            status = -1
        finally:
            if self._downloader.new_session and not self._downloader.session.closed:
                await self._downloader.session.close()
        return status

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
            # TODO: Add check for dataset updation
            self._dataset.to_zarr(f"{self._path}/{self._id}.zarr", mode="w")
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
