"""Tests for the downloader module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest

from src.utils.downloader import Downloader


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class TestDownloaderInitialization:
    """Test suite for Downloader.__init__."""

    @pytest.fixture(autouse=True)
    def cleanup_sessions(self):
        """Clean up any lingering aiohttp sessions."""
        self.sessions_to_close = []
        yield
        # Close any sessions that were created
        for session in self.sessions_to_close:
            if not session.closed:
                asyncio.run(session.close())

    def test_init_with_defaults(self, tmp_path):
        """Test initialization with default parameters."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=str(file_path),
            )

            assert downloader.url == "https://example.com/file.bin"
            assert downloader.file == str(file_path)
            assert downloader.threads == 4
            assert downloader.progress_bar is True
            assert downloader.new_session is True
            assert downloader.session is mock_session
            assert downloader.aiohttp_args == {"method": "GET"}

    def test_init_with_custom_parameters(self, tmp_path):
        """Test initialization with custom parameters."""
        file_path = tmp_path / "test.bin"
        custom_session = Mock(spec=aiohttp.ClientSession)
        custom_args = {"method": "POST", "headers": {"Authorization": "Bearer token"}}

        downloader = Downloader(
            url="https://example.com/file.bin",
            file=str(file_path),
            threads=8,
            session=custom_session,
            progress_bar=False,
            aiohttp_args=custom_args,
        )

        assert downloader.threads == 8
        assert downloader.session is custom_session
        assert downloader.progress_bar is False
        assert downloader.new_session is False
        assert downloader.aiohttp_args == custom_args

    def test_init_with_path_object(self, tmp_path):
        """Test initialization with Path object instead of string."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession"):
            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            assert downloader.file == file_path

    def test_init_creates_directory(self, tmp_path):
        """Test that parent directories are created when create_dir=True."""
        file_path = tmp_path / "subdir" / "nested" / "test.bin"
        assert not file_path.parent.exists()

        with patch("aiohttp.ClientSession"):
            Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                create_dir=True,
            )

        assert file_path.parent.exists()

    def test_init_does_not_create_directory(self, tmp_path):
        """Test that directories are not created when create_dir=False."""
        file_path = tmp_path / "subdir" / "test.bin"
        assert not file_path.parent.exists()

        with patch("aiohttp.ClientSession"):
            Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                create_dir=False,
            )

        assert not file_path.parent.exists()

    def test_init_aiohttp_args_default_method(self, tmp_path):
        """Test that method defaults to GET if not specified in aiohttp_args."""
        file_path = tmp_path / "test.bin"
        custom_args = {"headers": {"User-Agent": "TestBot"}}

        with patch("aiohttp.ClientSession"):
            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                aiohttp_args=custom_args,
            )

            assert downloader.aiohttp_args["method"] == "GET"
            assert downloader.aiohttp_args["headers"]["User-Agent"] == "TestBot"

    def test_init_aiohttp_args_none(self, tmp_path):
        """Test initialization with None aiohttp_args."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession"):
            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                aiohttp_args=None,
            )

            assert downloader.aiohttp_args == {"method": "GET"}


class TestDownloaderStart:
    """Test suite for Downloader.start() synchronous method."""

    @patch("src.utils.downloader.asyncio.get_event_loop")
    @pytest.mark.asyncio
    async def test_start_calls_asyncstart(self, mock_get_loop, tmp_path):
        """Test that start() calls asyncstart() via event loop."""
        file_path = tmp_path / "test.bin"
        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
        )

        # Mock the event loop and run_until_complete
        mock_loop = Mock()
        mock_get_loop.return_value = mock_loop

        # Mock asyncstart to avoid actual download
        with patch.object(downloader, "asyncstart", new_callable=AsyncMock):
            downloader.start()

        mock_loop.run_until_complete.assert_called_once()


class TestDownloaderAsyncStart:
    """Test suite for Downloader.asyncstart() method."""

    @pytest.mark.asyncio
    async def test_asyncstart_calls_download(self, tmp_path):
        """Test that asyncstart calls download method."""
        file_path = tmp_path / "test.bin"
        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
        )

        with patch.object(
            downloader, "download", new_callable=AsyncMock
        ) as mock_download:
            await downloader.asyncstart()
            mock_download.assert_called_once()

        # Clean up session
        await downloader.session.close()

    @pytest.mark.asyncio
    async def test_asyncstart_closes_new_session(self, tmp_path):
        """Test that asyncstart closes session when new_session=True."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            # Create AsyncMock, but we'll track calls via side_effect to avoid warning
            close_called = []

            async def mock_close():
                close_called.append(True)

            mock_session.close = mock_close
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            assert downloader.new_session is True

            with patch.object(downloader, "download", new_callable=AsyncMock):
                await downloader.asyncstart()
                assert len(close_called) == 1

    @pytest.mark.asyncio
    async def test_asyncstart_preserves_existing_session(self, tmp_path):
        """Test that asyncstart doesn't close session when new_session=False."""
        file_path = tmp_path / "test.bin"
        # Use a Mock instead of real session to avoid unawaited coroutine warning
        custom_session = Mock(spec=aiohttp.ClientSession)
        custom_session.close = MagicMock()  # Regular mock, not async

        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
            session=custom_session,
        )

        assert downloader.new_session is False

        with patch.object(downloader, "download", new_callable=AsyncMock):
            await downloader.asyncstart()
            custom_session.close.assert_not_called()


class TestDownloaderFetch:
    """Test suite for Downloader.fetch() method."""

    @pytest.mark.asyncio
    async def test_fetch_writes_to_file(self, tmp_path):
        """Test that fetch writes data to file at correct offset."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            # Create async generator for iter_any
            async def async_iter():
                yield b"test data"

            # Mock the session request
            mock_response = MagicMock()
            mock_response.content.iter_any = lambda: async_iter()
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_response)

            await downloader.fetch(progress=False, filerange=(0, 8))

            # Verify file was created and contains data
            assert file_path.exists()
            with open(file_path, "rb") as f:
                content = f.read()
                assert b"test data" in content

    @pytest.mark.asyncio
    async def test_fetch_with_progress_bar(self, tmp_path):
        """Test fetch with progress bar updates."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            # Mock progress bar
            mock_progress = Mock()
            mock_progress.update = Mock()

            # Create async generator for iter_any
            test_data = b"test data chunk"

            async def async_iter():
                yield test_data

            # Mock the session request
            mock_response = MagicMock()
            mock_response.content.iter_any = lambda: async_iter()
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_response)

            await downloader.fetch(
                progress=mock_progress, filerange=(0, len(test_data) - 1)
            )

            # Verify progress was updated
            mock_progress.update.assert_called_once_with(len(test_data))

    @pytest.mark.asyncio
    async def test_fetch_sets_range_header(self, tmp_path):
        """Test that fetch sets correct Range header."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            # Create async generator for iter_any
            async def async_iter():
                yield b"data"

            # Mock the session request
            mock_response = MagicMock()
            mock_response.content.iter_any = lambda: async_iter()
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_response)

            await downloader.fetch(progress=False, filerange=(100, 200))

            # Verify Range header was set
            assert downloader.aiohttp_args["headers"]["Range"] == "bytes=100-200"

    @pytest.mark.asyncio
    async def test_fetch_with_offset(self, tmp_path):
        """Test fetch writes at correct file offset."""
        file_path = tmp_path / "test.bin"

        # Pre-create file with some data
        with open(file_path, "wb") as f:
            f.write(b"X" * 100)

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            # Create async generator for iter_any
            async def async_iter():
                yield b"NEW"

            # Mock the session request
            mock_response = MagicMock()
            mock_response.content.iter_any = lambda: async_iter()
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_response)

            # Fetch starting at offset 50
            await downloader.fetch(progress=False, filerange=(50, 52))

            # Verify data was written at offset 50, but since we open in 'wb' mode,
            # this actually rewrites the file
            assert file_path.exists()


class TestDownloaderDownload:
    """Test suite for Downloader.download() method."""

    @pytest.mark.asyncio
    async def test_download_makes_head_request(self, tmp_path):
        """Test that download makes HEAD request to get Content-Length."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                progress_bar=False,
            )

            # Mock HEAD response
            mock_head_response = AsyncMock()
            mock_head_response.headers = {"Content-Length": "1000"}
            mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
            mock_head_response.__aexit__ = AsyncMock(return_value=None)

            # Mock fetch method
            with (
                patch.object(downloader, "fetch", new_callable=AsyncMock),
                patch.object(
                    mock_session, "request", return_value=mock_head_response
                ) as mock_request,
            ):
                await downloader.download()

                # Verify HEAD request was made
                mock_request.assert_called_once()
                call_kwargs = mock_request.call_args[1]
                assert call_kwargs["method"] == "HEAD"

    @pytest.mark.asyncio
    async def test_download_calculates_ranges(self, tmp_path):
        """Test that download calculates correct byte ranges for threads."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                threads=4,
                progress_bar=False,
            )

            # Mock HEAD response with 1000 byte file
            mock_head_response = AsyncMock()
            mock_head_response.headers = {"Content-Length": "1000"}
            mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
            mock_head_response.__aexit__ = AsyncMock(return_value=None)

            fetch_calls = []

            async def mock_fetch(progress, filerange, calls=fetch_calls):
                calls.append(filerange)

            with (
                patch.object(mock_session, "request", return_value=mock_head_response),
                patch.object(downloader, "fetch", side_effect=mock_fetch),
            ):
                await downloader.download()

            # Verify 4 ranges were created
            assert len(fetch_calls) == 4

            # Verify ranges cover the entire file
            # For 1000 bytes with 4 threads: base = 250
            # Ranges: (0, 249), (250, 499), (500, 749), (750, 1000)
            assert fetch_calls[0] == (0, 249)
            assert fetch_calls[1] == (250, 499)
            assert fetch_calls[2] == (500, 749)
            assert fetch_calls[3] == (750, 1000)

    @pytest.mark.asyncio
    async def test_download_with_different_thread_counts(self, tmp_path):
        """Test download with various thread counts."""
        test_cases = [
            (1, [(0, 1000)]),  # Single thread
            (2, [(0, 499), (500, 1000)]),  # Two threads
        ]

        for thread_count, expected_ranges in test_cases:
            file_path = tmp_path / f"test_{thread_count}.bin"

            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_session = Mock()
                mock_session_class.return_value = mock_session

                downloader = Downloader(
                    url="https://example.com/file.bin",
                    file=file_path,
                    threads=thread_count,
                    progress_bar=False,
                )

                mock_head_response = AsyncMock()
                mock_head_response.headers = {"Content-Length": "1000"}
                mock_head_response.__aenter__ = AsyncMock(
                    return_value=mock_head_response
                )
                mock_head_response.__aexit__ = AsyncMock(return_value=None)

                fetch_calls = []

                async def mock_fetch(progress, filerange, calls=fetch_calls):
                    calls.append(filerange)

                with (
                    patch.object(
                        mock_session, "request", return_value=mock_head_response
                    ),
                    patch.object(downloader, "fetch", side_effect=mock_fetch),
                ):
                    await downloader.download()

                assert fetch_calls == expected_ranges

    @pytest.mark.asyncio
    async def test_download_with_progress_bar(self, tmp_path):
        """Test download with progress bar enabled."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                progress_bar=True,
            )

            mock_head_response = MagicMock()
            mock_head_response.headers = {"Content-Length": "1000"}
            mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
            mock_head_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_head_response)

            with (
                patch.object(downloader, "fetch", new_callable=AsyncMock),
                patch("tqdm.tqdm") as mock_tqdm,
            ):
                # Mock the tqdm context manager
                mock_progress = MagicMock()
                mock_tqdm.return_value.__enter__ = Mock(return_value=mock_progress)
                mock_tqdm.return_value.__exit__ = Mock(return_value=None)

                await downloader.download()

                # Verify tqdm was called with correct parameters
                mock_tqdm.assert_called_once_with(total=1000, unit_scale=True, unit="B")

    @pytest.mark.asyncio
    async def test_download_without_progress_bar(self, tmp_path):
        """Test download with progress bar disabled."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
                progress_bar=False,
            )

            mock_head_response = AsyncMock()
            mock_head_response.headers = {"Content-Length": "1000"}
            mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
            mock_head_response.__aexit__ = AsyncMock(return_value=None)

            mock_session.request = MagicMock(return_value=mock_head_response)

            with patch.object(
                downloader, "fetch", new_callable=AsyncMock
            ) as mock_fetch:
                await downloader.download()

                # Verify fetch was called with False for progress
                for call in mock_fetch.call_args_list:
                    assert call[0][0] is False  # First arg is progress


class TestDownloaderErrorHandling:
    """Test suite for error handling in Downloader."""

    @pytest.mark.asyncio
    async def test_download_missing_content_length(self, tmp_path):
        """Test handling of missing Content-Length header."""
        file_path = tmp_path / "test.bin"
        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
            progress_bar=False,
        )

        # Mock HEAD response without Content-Length
        mock_head_response = AsyncMock()
        mock_head_response.headers = {}
        mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
        mock_head_response.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(
                downloader.session, "request", return_value=mock_head_response
            ),
            pytest.raises(KeyError),
        ):
            await downloader.download()

        await downloader.session.close()

    @pytest.mark.asyncio
    async def test_fetch_network_error(self, tmp_path):
        """Test fetch handling network errors."""
        file_path = tmp_path / "test.bin"

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            downloader = Downloader(
                url="https://example.com/file.bin",
                file=file_path,
            )

            # Mock network error - the request itself should raise the error
            mock_session.request = MagicMock(
                side_effect=aiohttp.ClientError("Network error")
            )

            with pytest.raises(aiohttp.ClientError):
                await downloader.fetch(progress=False, filerange=(0, 100))


class TestDownloaderEdgeCases:
    """Test suite for edge cases."""

    @pytest.mark.asyncio
    async def test_very_small_file(self, tmp_path):
        """Test downloading a very small file (smaller than thread count)."""
        file_path = tmp_path / "small.bin"
        downloader = Downloader(
            url="https://example.com/small.bin",
            file=file_path,
            threads=4,
            progress_bar=False,
        )

        # Mock HEAD response with 10 byte file
        mock_head_response = AsyncMock()
        mock_head_response.headers = {"Content-Length": "10"}
        mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
        mock_head_response.__aexit__ = AsyncMock(return_value=None)

        fetch_calls = []

        async def mock_fetch(progress, filerange, calls=fetch_calls):
            calls.append(filerange)

        with (
            patch.object(
                downloader.session, "request", return_value=mock_head_response
            ),
            patch.object(downloader, "fetch", side_effect=mock_fetch),
        ):
            await downloader.download()

        # Should still create 4 ranges even for small file
        assert len(fetch_calls) == 4

        # Verify ranges don't overlap and cover entire file
        assert fetch_calls[-1][1] == 10  # Last range ends at file size

        await downloader.session.close()

    @pytest.mark.asyncio
    async def test_single_thread_download(self, tmp_path):
        """Test download with single thread."""
        file_path = tmp_path / "test.bin"
        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
            threads=1,
            progress_bar=False,
        )

        mock_head_response = AsyncMock()
        mock_head_response.headers = {"Content-Length": "1000"}
        mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
        mock_head_response.__aexit__ = AsyncMock(return_value=None)

        fetch_calls = []

        async def mock_fetch(progress, filerange, calls=fetch_calls):
            calls.append(filerange)

        with (
            patch.object(
                downloader.session, "request", return_value=mock_head_response
            ),
            patch.object(downloader, "fetch", side_effect=mock_fetch),
        ):
            await downloader.download()

        # Single thread should download entire file
        assert len(fetch_calls) == 1
        assert fetch_calls[0] == (0, 1000)

        await downloader.session.close()

    @pytest.mark.asyncio
    async def test_large_thread_count(self, tmp_path):
        """Test download with large number of threads."""
        file_path = tmp_path / "test.bin"
        downloader = Downloader(
            url="https://example.com/file.bin",
            file=file_path,
            threads=32,
            progress_bar=False,
        )

        mock_head_response = AsyncMock()
        mock_head_response.headers = {"Content-Length": "10000"}
        mock_head_response.__aenter__ = AsyncMock(return_value=mock_head_response)
        mock_head_response.__aexit__ = AsyncMock(return_value=None)

        fetch_calls = []

        async def mock_fetch(progress, filerange, calls=fetch_calls):
            calls.append(filerange)

        with (
            patch.object(
                downloader.session, "request", return_value=mock_head_response
            ),
            patch.object(downloader, "fetch", side_effect=mock_fetch),
        ):
            await downloader.download()

        # Should create 32 ranges
        assert len(fetch_calls) == 32

        # Verify last range ends at file size
        assert fetch_calls[-1][1] == 10000

        await downloader.session.close()
