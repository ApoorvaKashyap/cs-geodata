"""An optionally asynchronous multi-threaded downloader module for Python.
- Source: https://github.com/DashLt/multithread/blob/master/multithread/__init__.py
- License: MIT License
"""

import asyncio
from pathlib import Path

import aiofiles
import aiohttp

name = "multithread"
__version__ = "1.0.1"


class Downloader:
    """
    An optionally asynchronous multi-threaded downloader class using aiohttp

    Attributes:

        - url (str): The URL to download
        - file (str or path-like object): The filename to write the download to.
        - threads (int): The number of threads to use to download
        - session (aiohttp.ClientSession): An existing session to use with aiohttp
        - new_session (bool): True if a session was not passed, a new one was created
        - progress_bar (bool): Whether to output a progress bar or not
        - aiohttp_args (dict): Arguments to be passed in each aiohttp request.
        If you supply a Range header using this, it will be overwritten in fetch()
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
        """Assigns arguments to self for when asyncstart() or start() calls download.

        All arguments are assigned directly to self except for:

            - session: if not passed, a ClientSession is created
            - aiohttp_args: if the key "method" does not exist, it is set to "GET"
            - create_dir: see parameter description

        Parameters:

            - url (str): The URL to download
            - file (str or path-like object): The filename to write the download to.
            - threads (int): The number of threads to use to download
            - session (aiohttp.ClientSession): An existing session to use with aiohttp
            - progress_bar (bool): Whether to output a progress bar or not
            - aiohttp_args (dict): Arguments to be passed in each aiohttp request. If you supply a Range header using this, it will be overwritten in fetch()
            - create_dir (bool): If true, the directories encompassing the file will be created if they do not exist already.
        """  # noqa: E501
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
        """Calls asyncstart() synchronously"""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.asyncstart())

    async def asyncstart(self) -> None:
        """Re-initializes file and calls download() with it. Closes session if necessary"""  # noqa: E501
        try:
            await self.download()
        finally:
            if self.new_session:
                await self.session.close()

    async def fetch(self, progress: bool = False, filerange: tuple = (0, "")) -> None:
        """Individual thread for fetching files.

        Parameters:

            - progress (bool or tqdm.Progress): the progress bar (or lack thereof) to update
            - filerange (tuple): the range of the file to get
        """  # noqa: E501
        async with aiofiles.open(self.file, "r+b") as fileobj:
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
        """Generates ranges and calls fetch() with them."""
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
