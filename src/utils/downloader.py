"""Asynchronous multi-threaded downloader module for Python.

This module provides the Downloader class for efficiently downloading files using
multiple threads and asynchronous I/O. It uses aiohttp for HTTP requests and
aiofiles for asynchronous file operations.

Source:
    https://github.com/DashLt/multithread/blob/master/multithread/__init__.py

License:
    MIT License

Attributes:
    name: Module name identifier.
    __version__: Current version of the module.
"""

import asyncio
from pathlib import Path

import aiofiles
import aiohttp

name = "multithread"
__version__ = "1.0.1"


class Downloader:
    """Asynchronous multi-threaded file downloader using aiohttp.

    This class downloads files from a URL using multiple threads to improve
    download speed. It supports both synchronous and asynchronous interfaces,
    with optional progress bar display.

    Attributes:
        url: The URL to download from.
        file: The file path to write the downloaded content to.
        threads: The number of concurrent threads to use for downloading.
        session: The aiohttp ClientSession for making HTTP requests.
        new_session: True if a new session was created internally, False if
            a session was provided.
        progress_bar: Whether to display a progress bar during download.
        aiohttp_args: Additional arguments to pass to aiohttp requests.
            Note: Range headers will be overwritten by the fetch method.
    """

    def __init__(
        self,
        url: str,
        file: str | Path,
        threads: int = 4,
        session: aiohttp.ClientSession | None = None,
        progress_bar: bool = True,
        aiohttp_args: dict | None = None,
        create_dir: bool = True,
    ) -> None:
        """Initialize the Downloader instance.

        Sets up the downloader with the specified URL, file path, and options.
        Creates parent directories if needed and initializes or reuses an
        aiohttp session.

        Args:
            url: The URL to download from.
            file: The file path to write the downloaded content to.
            threads: The number of concurrent threads to use. Defaults to 4.
            session: An existing aiohttp ClientSession to reuse. If None, a new
                session will be created. Defaults to None.
            progress_bar: Whether to display a download progress bar using tqdm.
                Defaults to True.
            aiohttp_args: Additional keyword arguments to pass to aiohttp requests.
                If 'method' is not specified, it defaults to 'GET'. Any Range
                header will be overwritten during download. Defaults to None.
            create_dir: Whether to create parent directories for the file if they
                don't exist. Defaults to True.
        """
        if aiohttp_args is None:
            aiohttp_args = {"method": "GET"}
        self.url = url
        if create_dir:
            parent_directory = Path(file).parent
            parent_directory.mkdir(parents=True, exist_ok=True)
        self.file = file
        self.threads = threads
        if not session:
            self.session = aiohttp.ClientSession()
            self.new_session = True
        else:
            self.session = session
            self.new_session = False
        self.progress_bar = progress_bar
        if "method" not in aiohttp_args:
            aiohttp_args["method"] = "GET"
        self.aiohttp_args = aiohttp_args

    def start(self) -> None:
        """Start the download synchronously.

        This is a blocking call that runs the asynchronous download process
        in the event loop until completion.
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.asyncstart())

    async def asyncstart(self) -> None:
        """Start the download asynchronously.

        Initiates the download process and ensures the session is closed
        if it was created internally.
        """
        await self.download()
        if self.new_session:
            await self.session.close()

    async def fetch(self, progress: bool = False, filerange: tuple = (0, "")) -> None:
        """Fetch a specific byte range of the file.

        This method is called by multiple threads concurrently, each fetching
        a different portion of the file based on the specified byte range.

        Args:
            progress: The tqdm progress bar instance to update, or False if no
                progress bar should be used. Defaults to False.
            filerange: A tuple specifying the byte range to fetch in the format
                (start_byte, end_byte). Defaults to (0, "").
        """
        async with aiofiles.open(self.file, "wb") as fileobj:
            if "headers" not in self.aiohttp_args:
                self.aiohttp_args["headers"] = {}
            self.aiohttp_args["headers"]["Range"] = (
                f"bytes={filerange[0]}-{filerange[1]}"
            )
            async with self.session.request(
                url=self.url, **self.aiohttp_args
            ) as filereq:
                offset = filerange[0]
                await fileobj.seek(offset)
                async for chunk in filereq.content.iter_any():
                    if progress:
                        progress.update(len(chunk))
                    await fileobj.write(chunk)

    async def download_single_threaded(self) -> None:
        """Download file in a single thread when Content-Length is not available."""
        async with self.session.request(url=self.url, **self.aiohttp_args) as response:
            async with aiofiles.open(self.file, "wb") as fileobj:
                if self.progress_bar:
                    from tqdm import tqdm

                    # Try to get length from headers, otherwise use unknown
                    total = None
                    if "Content-Length" in response.headers:
                        total = int(response.headers["Content-Length"])

                    with tqdm(total=total, unit_scale=True, unit="B") as progress:
                        async for chunk in response.content.iter_any():
                            progress.update(len(chunk))
                            await fileobj.write(chunk)
                else:
                    async for chunk in response.content.iter_any():
                        await fileobj.write(chunk)

    async def download(self) -> None:
        """Download the file using multiple concurrent threads.

        Performs a HEAD request to get the file size, divides it into ranges
        based on the number of threads, and initiates concurrent fetch operations
        for each range. Optionally displays a progress bar.

        Raises:
            KeyError: If the Content-Length header is missing from the HEAD response.
            aiohttp.ClientError: If there are issues with HTTP requests.
        """
        temp_args = self.aiohttp_args.copy()
        temp_args["method"] = "HEAD"
        async with self.session.request(url=self.url, **temp_args) as head:
            # Check if Content-Length header exists
            if "Content-Length" not in head.headers:
                # Fall back to single-threaded download
                await self.download_single_threaded()
                return

            length = int(head.headers["Content-Length"])

            # Create the file with the correct size before multi-threaded writes
            async with aiofiles.open(self.file, "wb") as f:
                await f.write(b"\0" * length)

            start = -1
            base = int(length / self.threads)
            ranges = []
            for _ in range(self.threads - 1):
                ranges.append((start + 1, start + base))
                start += base
            ranges.append((start + 1, length))
            if self.progress_bar:
                from tqdm import tqdm

                with tqdm(total=length, unit_scale=True, unit="B") as progress:
                    await asyncio.gather(
                        *[self.fetch(progress, filerange) for filerange in ranges]
                    )
            else:
                await asyncio.gather(
                    *[self.fetch(False, filerange) for filerange in ranges]
                )
