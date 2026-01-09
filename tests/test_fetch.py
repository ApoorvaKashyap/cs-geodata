import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
from requests.exceptions import ConnectionError, RequestException

from src.fetch import LayersFetch


class TestLayersFetch:
    @pytest.fixture
    def sample_themes_data(self):
        """Sample themes data for testing."""
        return {
            "hydrology": [
                {
                    "name": "{{state}}_{{district}}_{{tehsil}}_stream",
                    "stac_uri": "https://api.example.com/{{state}}/{{district}}/{{tehsil}}/stream.json",
                    "type": "geotiff",
                },
                {
                    "name": "{{state}}_{{district}}_{{tehsil}}_drainage",
                    "stac_uri": "https://api.example.com/{{state}}/{{district}}/{{tehsil}}/drainage.json",
                    "type": "geojson",
                },
            ],
            "climate": [
                {
                    "name": "{{state}}_{{district}}_{{tehsil}}_drought",
                    "stac_uri": "https://api.example.com/{{state}}/{{district}}/{{tehsil}}/drought.json",
                    "type": "geojson",
                }
            ],
        }

    @pytest.fixture
    def themes_file(self, sample_themes_data, tmp_path):
        """Create a temporary themes JSON file for testing."""
        themes_path = tmp_path / "themes.json"
        with open(themes_path, "w") as f:
            json.dump(sample_themes_data, f)
        return str(themes_path)

    @pytest.fixture
    def layers_fetch_instance(self, themes_file):
        """Create a LayersFetch instance with test data."""
        return LayersFetch(
            state="TestState",
            district="TestDistrict",
            tehsil="TestTehsil",
            themes_file=themes_file,
        )

    @pytest.fixture
    def geotiff_response(self):
        """Sample STAC response for GeoTIFF data."""
        return {
            "assets": {
                "data": {
                    "type": "image/tiff; application=geotiff",
                    "href": "https://example.com/data.tif",
                }
            }
        }

    @pytest.fixture
    def geojson_response(self):
        """Sample STAC response for GeoJSON data."""
        return {
            "assets": {
                "data": {
                    "type": "application/geo+json",
                    "href": "https://example.com/data.geojson",
                }
            }
        }

    @pytest.fixture
    def unknown_response(self):
        """Sample STAC response for unknown data type."""
        return {
            "assets": {
                "data": {
                    "type": "application/unknown",
                    "href": "https://example.com/data.unknown",
                }
            }
        }

    # Test initialization
    def test_init_with_default_themes_file(self):
        """Test LayersFetch initialization with default themes file."""
        # This test requires the actual themes file to exist
        themes_file_path = Path("data/themes.json")
        if themes_file_path.exists():
            fetcher = LayersFetch(state="Maharashtra", district="Pune", tehsil="Haveli")
            assert fetcher._state == "Maharashtra"
            assert fetcher._district == "Pune"
            assert fetcher._tehsil == "Haveli"
            assert fetcher._themes is not None

    def test_init_with_custom_themes_file(self, themes_file):
        """Test LayersFetch initialization with custom themes file."""
        fetcher = LayersFetch(
            state="Karnataka",
            district="Bangalore",
            tehsil="North",
            themes_file=themes_file,
        )
        assert fetcher._state == "Karnataka"
        assert fetcher._district == "Bangalore"
        assert fetcher._tehsil == "North"
        assert fetcher._themes is not None

    def test_init_stores_parameters(self, layers_fetch_instance):
        """Test that initialization stores all parameters correctly."""
        assert layers_fetch_instance._state == "TestState"
        assert layers_fetch_instance._district == "TestDistrict"
        assert layers_fetch_instance._tehsil == "TestTehsil"

    # Test parse_response method
    @pytest.mark.asyncio
    @patch("src.fetch.GeoTiffHandler")
    async def test_parse_response_geotiff(
        self, mock_handler_class, layers_fetch_instance, geotiff_response
    ):
        """Test parse_response with GeoTIFF data type."""
        from unittest.mock import AsyncMock

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        geotiff_response["id"] = "test-id"
        result = await layers_fetch_instance.parse_response(geotiff_response)

        mock_handler_class.assert_called_once()
        mock_handler.handle.assert_called_once()
        assert result == 1

    @pytest.mark.asyncio
    @patch("src.fetch.GeoJSONHandler")
    async def test_parse_response_geojson(
        self, mock_handler_class, layers_fetch_instance, geojson_response
    ):
        """Test parse_response with GeoJSON data type."""
        from unittest.mock import AsyncMock

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        geojson_response["id"] = "test-id"
        result = await layers_fetch_instance.parse_response(geojson_response)

        mock_handler_class.assert_called_once()
        mock_handler.handle.assert_called_once()
        assert result == 1

    @pytest.mark.asyncio
    async def test_parse_response_unknown_type(
        self, layers_fetch_instance, unknown_response
    ):
        """Test parse_response with unknown data type."""
        result = await layers_fetch_instance.parse_response(unknown_response)
        assert result == -1

    # Test fetch method
    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    @patch("src.fetch.GeoTiffHandler")
    async def test_fetch_success_single_layer(
        self, mock_handler_class, mock_get, layers_fetch_instance, geotiff_response
    ):
        """Test successful fetch for a single layer."""
        from unittest.mock import AsyncMock

        # Setup mocks
        geotiff_response["id"] = "test-id"
        mock_response = Mock()
        mock_response.json.return_value = geotiff_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        # Execute
        result = await layers_fetch_instance.fetch("hydrology")

        # Verify
        assert result == 1
        assert mock_get.call_count == 2  # hydrology has 2 layers

        # Verify URL replacement happened correctly
        first_call_url = mock_get.call_args_list[0][0][0]
        assert "teststate" in first_call_url
        assert "testdistrict" in first_call_url
        assert "testtehsil" in first_call_url

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    async def test_fetch_connection_error(
        self, mock_get, layers_fetch_instance, capsys
    ):
        """Test fetch handling ConnectionError."""
        mock_get.side_effect = ConnectionError("Connection failed")

        result = await layers_fetch_instance.fetch("hydrology")

        assert result == -1
        captured = capsys.readouterr()
        assert "Connection Error Occurred" in captured.out

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    async def test_fetch_request_exception(
        self, mock_get, layers_fetch_instance, capsys
    ):
        """Test fetch handling RequestException."""
        mock_get.side_effect = RequestException("Request failed")

        result = await layers_fetch_instance.fetch("hydrology")

        assert result == -1
        captured = capsys.readouterr()
        assert "Request Exception Occurred" in captured.out

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    async def test_fetch_generic_exception(
        self, mock_get, layers_fetch_instance, capsys
    ):
        """Test fetch handling generic Exception."""
        mock_get.side_effect = Exception("Unexpected error")

        result = await layers_fetch_instance.fetch("hydrology")

        assert result == -1
        captured = capsys.readouterr()
        assert "Unexpected Error Occurred" in captured.out

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    @patch("src.fetch.GeoJSONHandler")
    async def test_fetch_multiple_layers(
        self, mock_handler_class, mock_get, layers_fetch_instance, geojson_response
    ):
        """Test fetch with multiple layers in a theme."""
        from unittest.mock import AsyncMock

        # Setup mocks
        geojson_response["id"] = "test-id"
        mock_response = Mock()
        mock_response.json.return_value = geojson_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        # Execute
        result = await layers_fetch_instance.fetch("hydrology")

        # Verify - hydrology has 2 layers
        assert result == 1
        assert mock_get.call_count == 2

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    async def test_fetch_nonexistent_theme(self, mock_get, layers_fetch_instance):
        """Test fetch with nonexistent theme name."""
        # This should handle None returned from get_theme
        # The current implementation will iterate over None and fail
        # We need to test the actual behavior
        with pytest.raises(TypeError):
            await layers_fetch_instance.fetch("nonexistent")

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    @patch("src.fetch.GeoJSONHandler")
    async def test_fetch_url_template_replacement(
        self, mock_handler_class, mock_get, layers_fetch_instance, geojson_response
    ):
        """Test that URL templates are correctly replaced with actual values."""
        from unittest.mock import AsyncMock

        geojson_response["id"] = "test-id"
        mock_response = Mock()
        mock_response.json.return_value = geojson_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        await layers_fetch_instance.fetch("climate")

        # Check that the URL was called with lowercased replacements
        called_url = mock_get.call_args[0][0]
        assert "{{state}}" not in called_url
        assert "{{district}}" not in called_url
        assert "{{tehsil}}" not in called_url
        assert "teststate" in called_url
        assert "testdistrict" in called_url
        assert "testtehsil" in called_url

    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    @patch("src.fetch.GeoTiffHandler")
    async def test_fetch_http_error(
        self, mock_handler_class, mock_get, layers_fetch_instance, geotiff_response
    ):
        """Test fetch handling HTTP errors via raise_for_status."""
        from unittest.mock import AsyncMock

        geotiff_response["id"] = "test-id"
        mock_response = Mock()
        mock_response.json.return_value = geotiff_response
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        mock_handler = MagicMock()
        mock_handler.handle = AsyncMock(return_value=1)
        mock_handler_class.return_value = mock_handler

        result = await layers_fetch_instance.fetch("hydrology")

        # HTTPError is a subclass of RequestException, so it should return -1
        assert result == -1

    # Integration tests
    @pytest.mark.asyncio
    @patch("src.fetch.requests.get")
    @patch("src.fetch.GeoTiffHandler")
    @patch("src.fetch.GeoJSONHandler")
    async def test_fetch_mixed_data_types(
        self,
        mock_geojson_handler,
        mock_geotiff_handler,
        mock_get,
        layers_fetch_instance,
    ):
        """Test fetch with mixed GeoTIFF and GeoJSON layers."""
        from unittest.mock import AsyncMock

        # Setup responses for different data types
        geotiff_resp = {
            "id": "test-id-1",
            "assets": {
                "data": {
                    "type": "image/tiff; application=geotiff",
                    "href": "https://example.com/data.tif",
                }
            },
        }
        geojson_resp = {
            "id": "test-id-2",
            "assets": {
                "data": {
                    "type": "application/geo+json",
                    "href": "https://example.com/data.geojson",
                }
            },
        }

        mock_response1 = Mock()
        mock_response1.json.return_value = geotiff_resp
        mock_response1.raise_for_status.return_value = None

        mock_response2 = Mock()
        mock_response2.json.return_value = geojson_resp
        mock_response2.raise_for_status.return_value = None

        mock_get.side_effect = [mock_response1, mock_response2]

        mock_geotiff = MagicMock()
        mock_geotiff.handle = AsyncMock(return_value=1)
        mock_geotiff_handler.return_value = mock_geotiff

        mock_geojson = MagicMock()
        mock_geojson.handle = AsyncMock(return_value=1)
        mock_geojson_handler.return_value = mock_geojson

        # Execute - hydrology theme has both geotiff and geojson
        result = await layers_fetch_instance.fetch("hydrology")

        # Verify both handlers were called
        assert result == 1
        mock_geotiff_handler.assert_called_once()
        mock_geojson_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_case_sensitivity(self, themes_file):
        """Test that state, district, and tehsil are lowercased in URLs."""
        from unittest.mock import AsyncMock

        fetcher = LayersFetch(
            state="UPPERCASE",
            district="MixedCase",
            tehsil="lowercase",
            themes_file=themes_file,
        )

        with patch("src.fetch.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {
                "id": "test-id",
                "assets": {
                    "data": {
                        "type": "application/geo+json",
                        "href": "https://example.com/data.geojson",
                    }
                },
            }
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with patch("src.fetch.GeoJSONHandler") as mock_handler_class:
                mock_handler = MagicMock()
                mock_handler.handle = AsyncMock(return_value=1)
                mock_handler_class.return_value = mock_handler

                await fetcher.fetch("climate")

                # Verify URL is lowercased
                called_url = mock_get.call_args[0][0]
                assert "uppercase" in called_url
                assert "mixedcase" in called_url
                assert "lowercase" in called_url
                assert "UPPERCASE" not in called_url
                assert "MixedCase" not in called_url
