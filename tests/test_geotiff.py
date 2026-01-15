"""Tests for the GeoTIFF handler module."""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
import xarray as xr

from src.handlers.geotiff import GeoTiffHandler


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_dataset():
    """Create a mock xarray Dataset for testing."""
    # Create a complete mock instead of a real DataArray
    # because xr.DataArray.rio is a read-only accessor
    dataset = Mock(spec=xr.DataArray)
    dataset.name = None
    dataset.dims = ("band", "y", "x")

    # Mock the rio accessor
    dataset.rio = Mock()
    dataset.rio.bounds = Mock(return_value=(0, 0, 1, 1))
    dataset.to_zarr = Mock()

    return dataset


class TestGeoTiffHandlerInitialization:
    """Test suite for GeoTiffHandler.__init__."""

    @patch("src.handlers.geotiff.Downloader")
    def test_init_with_defaults(self, mock_downloader_class, tmp_path):
        """Test initialization with default parameters."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        assert handler._id == "test-id"
        assert handler._url == "https://example.com/file.tif"
        assert handler._path == "/tmp/geotiff"
        mock_downloader_class.assert_called_once_with(
            "https://example.com/file.tif", "/tmp/geotiff/test-id.tif"
        )

    @patch("src.handlers.geotiff.Downloader")
    def test_init_with_custom_path(self, mock_downloader_class, tmp_path):
        """Test initialization with custom path."""
        custom_path = str(tmp_path / "custom")
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
            path=custom_path,
        )

        assert handler._path == custom_path
        mock_downloader_class.assert_called_once_with(
            "https://example.com/file.tif", f"{custom_path}/test-id.tif"
        )

    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.xr.set_options")
    def test_init_sets_xarray_options(self, mock_set_options, mock_downloader_class):
        """Test that initialization sets xarray options."""
        GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        mock_set_options.assert_called_once_with(keep_attrs=True)

    @patch("src.handlers.geotiff.Downloader")
    def test_dataset_initially_none(self, mock_downloader_class):
        """Test that _dataset is initially None."""
        GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        # Class attribute should be None
        assert GeoTiffHandler._dataset is None


class TestGeoTiffHandlerDownload:
    """Test suite for GeoTiffHandler._download()."""

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_download_success(self, mock_downloader_class):
        """Test successful download using asyncstart."""
        mock_downloader = Mock()
        mock_downloader.asyncstart = AsyncMock()
        mock_downloader_class.return_value = mock_downloader

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._download()

        assert status == 1
        mock_downloader.asyncstart.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.Path")
    async def test_download_with_value_error_fallback(
        self, mock_path_class, mock_downloader_class
    ):
        """Test download falls back to download() on ValueError."""
        # Mock Path to simulate file doesn't exist
        mock_path = Mock()
        mock_path.is_file.return_value = False
        mock_path_class.return_value = mock_path

        mock_downloader = Mock()
        mock_downloader.asyncstart = AsyncMock(side_effect=ValueError("test error"))
        mock_downloader.download = AsyncMock()
        mock_downloader.new_session = True
        mock_downloader.session = Mock()
        mock_downloader.session.closed = False
        mock_downloader.session.close = AsyncMock()
        mock_downloader_class.return_value = mock_downloader

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._download()

        assert status == 1
        mock_downloader.asyncstart.assert_called_once()
        mock_downloader.download.assert_called_once()
        mock_downloader.session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_download_value_error_no_session_close(self, mock_downloader_class):
        """Test download doesn't close session when new_session=False."""
        mock_downloader = Mock()
        mock_downloader.asyncstart = AsyncMock(side_effect=ValueError("test error"))
        mock_downloader.download = AsyncMock()
        mock_downloader.new_session = False
        mock_downloader.session = Mock()
        mock_downloader.session.close = AsyncMock()
        mock_downloader_class.return_value = mock_downloader

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._download()

        assert status == 1
        mock_downloader.session.close.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_download_with_exception(self, mock_downloader_class, capsys):
        """Test download handles exceptions and returns -1."""
        mock_downloader = Mock()
        mock_downloader.asyncstart = AsyncMock(side_effect=Exception("Network error"))
        mock_downloader.new_session = True
        mock_downloader.session = Mock()
        mock_downloader.session.closed = False
        mock_downloader.session.close = AsyncMock()
        mock_downloader_class.return_value = mock_downloader

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._download()

        assert status == -1
        mock_downloader.session.close.assert_called_once()
        captured = capsys.readouterr()
        assert "Error downloading GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_download_exception_closed_session(self, mock_downloader_class):
        """Test download doesn't close already closed session."""
        mock_downloader = Mock()
        mock_downloader.asyncstart = AsyncMock(side_effect=Exception("Network error"))
        mock_downloader.new_session = True
        mock_downloader.session = Mock()
        mock_downloader.session.closed = True
        mock_downloader.session.close = AsyncMock()
        mock_downloader_class.return_value = mock_downloader

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._download()

        assert status == -1
        mock_downloader.session.close.assert_not_called()


class TestGeoTiffHandlerLoad:
    """Test suite for GeoTiffHandler._load()."""

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.rxr.open_rasterio")
    async def test_load_success(
        self, mock_open_rasterio, mock_downloader_class, mock_dataset, capsys
    ):
        """Test successful loading of GeoTIFF file."""
        mock_open_rasterio.return_value = mock_dataset

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._load()

        assert status == 1
        assert handler._dataset is mock_dataset
        mock_open_rasterio.assert_called_once_with("/tmp/geotiff/test-id.tif")
        captured = capsys.readouterr()
        assert "Dimensions:" in captured.out
        assert "Bounding Box:" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.rxr.open_rasterio")
    async def test_load_with_custom_path(
        self, mock_open_rasterio, mock_downloader_class, mock_dataset, tmp_path
    ):
        """Test loading with custom path."""
        custom_path = str(tmp_path / "custom")
        mock_open_rasterio.return_value = mock_dataset

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
            path=custom_path,
        )

        status = await handler._load()

        assert status == 1
        mock_open_rasterio.assert_called_once_with(f"{custom_path}/test-id.tif")

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.rxr.open_rasterio")
    async def test_load_file_not_found(
        self, mock_open_rasterio, mock_downloader_class, capsys
    ):
        """Test load handles file not found error."""
        mock_open_rasterio.side_effect = FileNotFoundError("File not found")

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._load()

        assert status == -1
        captured = capsys.readouterr()
        assert "Error loading GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    @patch("src.handlers.geotiff.rxr.open_rasterio")
    async def test_load_with_exception(
        self, mock_open_rasterio, mock_downloader_class, capsys
    ):
        """Test load handles general exceptions."""
        mock_open_rasterio.side_effect = Exception("Unexpected error")

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._load()

        assert status == -1
        captured = capsys.readouterr()
        assert "Error loading GeoTIFF file" in captured.out
        assert "Unexpected error" in captured.out


class TestGeoTiffHandlerSave:
    """Test suite for GeoTiffHandler._save()."""

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_save_success(self, mock_downloader_class, mock_dataset):
        """Test successful save to Zarr format."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )
        handler._dataset = mock_dataset

        status = await handler._save()

        assert status == 1
        mock_dataset.to_zarr.assert_called_once_with(
            "/tmp/geotiff/test-id.zarr", mode="w"
        )

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_save_with_custom_path(
        self, mock_downloader_class, mock_dataset, tmp_path
    ):
        """Test save with custom path."""
        custom_path = str(tmp_path / "custom")
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
            path=custom_path,
        )
        handler._dataset = mock_dataset

        status = await handler._save()

        assert status == 1
        mock_dataset.to_zarr.assert_called_once_with(
            f"{custom_path}/test-id.zarr", mode="w"
        )

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_save_with_exception(
        self, mock_downloader_class, mock_dataset, capsys
    ):
        """Test save handles exceptions."""
        mock_dataset.to_zarr.side_effect = Exception("Write error")

        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )
        handler._dataset = mock_dataset

        status = await handler._save()

        assert status == -1
        captured = capsys.readouterr()
        assert "Error saving GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_save_without_loaded_dataset(self, mock_downloader_class, capsys):
        """Test save fails gracefully when dataset is None."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        status = await handler._save()

        assert status == -1
        captured = capsys.readouterr()
        assert "Error saving GeoTIFF file" in captured.out


class TestGeoTiffHandlerHandle:
    """Test suite for GeoTiffHandler.handle() integration."""

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_full_workflow_success(
        self, mock_downloader_class, mock_dataset, capsys
    ):
        """Test complete workflow: download, load, save."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with (
            patch.object(handler, "_download", return_value=1) as mock_download,
            patch.object(handler, "_load", return_value=1) as mock_load,
            patch.object(handler, "_save", return_value=1) as mock_save,
        ):
            status = await handler.handle()

        assert status == 1
        mock_download.assert_called_once()
        mock_load.assert_called_once()
        mock_save.assert_called_once()

        captured = capsys.readouterr()
        assert "Downloading GeoTIFF file..." in captured.out
        assert "GeoTIFF file downloaded successfully" in captured.out
        assert "Loading GeoTIFF file..." in captured.out
        assert "GeoTIFF file loaded successfully" in captured.out
        assert "Saving GeoTIFF file..." in captured.out
        assert "GeoTIFF file saved successfully" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_download_fails(self, mock_downloader_class, capsys):
        """Test handle returns -1 when download fails."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with patch.object(handler, "_download", return_value=-1):
            status = await handler.handle()

        assert status == -1
        captured = capsys.readouterr()
        assert "Failed to download GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_load_fails(self, mock_downloader_class, capsys):
        """Test handle returns -1 when load fails."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with (
            patch.object(handler, "_download", return_value=1),
            patch.object(handler, "_load", return_value=-1),
        ):
            status = await handler.handle()

        assert status == -1
        captured = capsys.readouterr()
        assert "Failed to load GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_save_fails(self, mock_downloader_class, capsys):
        """Test handle returns -1 when save fails."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with (
            patch.object(handler, "_download", return_value=1),
            patch.object(handler, "_load", return_value=1),
            patch.object(handler, "_save", return_value=-1),
        ):
            status = await handler.handle()

        assert status == -1
        captured = capsys.readouterr()
        assert "Failed to save GeoTIFF file" in captured.out

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_stops_on_download_failure(self, mock_downloader_class):
        """Test that handle stops execution if download fails."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with (
            patch.object(handler, "_download", return_value=-1) as mock_download,
            patch.object(handler, "_load", return_value=1) as mock_load,
            patch.object(handler, "_save", return_value=1) as mock_save,
        ):
            status = await handler.handle()

        assert status == -1
        mock_download.assert_called_once()
        # Load and save should not be called
        mock_load.assert_not_called()
        mock_save.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.handlers.geotiff.Downloader")
    async def test_handle_stops_on_load_failure(self, mock_downloader_class):
        """Test that handle stops execution if load fails."""
        handler = GeoTiffHandler(
            id="test-id",
            url="https://example.com/file.tif",
        )

        with (
            patch.object(handler, "_download", return_value=1) as mock_download,
            patch.object(handler, "_load", return_value=-1) as mock_load,
            patch.object(handler, "_save", return_value=1) as mock_save,
        ):
            status = await handler.handle()

        assert status == -1
        mock_download.assert_called_once()
        mock_load.assert_called_once()
        # Save should not be called
        mock_save.assert_not_called()
