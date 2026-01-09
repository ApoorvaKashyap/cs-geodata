import requests
from requests.exceptions import ConnectionError, RequestException

from .categorizer import Theme
from .handlers.geojson import GeoJSONHandler
from .handlers.geotiff import GeoTiffHandler


class LayersFetch:
    """Fetch and process geospatial layers from STAC catalogs.

    This class handles fetching geospatial data layers from STAC
    APIs based on themes, geographic location (state, district, tehsil), and data types.
    It supports both GeoTIFF and GeoJSON formats.

    Attributes:
        _themes (Theme): Theme manager for accessing layer configurations.
        _state (str): State name for geographic filtering.
        _district (str): District name for geographic filtering.
        _tehsil (str): Tehsil/sub-district name for geographic filtering.
    """

    def __init__(
        self,
        state: str,
        district: str,
        tehsil: str,
        themes_file: str = "data/themes.json",
    ) -> None:
        """Initialize the LayersFetch instance.

        Args:
            state: Name of the state for geographic filtering.
            district: Name of the district for geographic filtering.
            tehsil: Name of the tehsil/sub-district for geographic filtering.
            themes_file: Path to the themes configuration JSON file.
                Defaults to "data/themes.json".

        Raises:
            FileNotFoundError: If the themes file does not exist.
            json.JSONDecodeError: If the themes file contains invalid JSON.
        """
        self._themes = Theme(themes_file)
        self._state = state
        self._district = district
        self._tehsil = tehsil

    async def parse_response(self, response: dict) -> int:
        """Parse STAC API response and delegate to appropriate handler.

        Examines the response from a STAC API to determine the data type,
        then creates and invokes the appropriate handler (GeoTIFF or GeoJSON)
        to process the data.

        Args:
            response: STAC API response dictionary containing asset information.
                Expected structure:
                {
                    "assets": {
                        "data": {
                            "type": "<mime-type>",
                            "href": "<url-to-data>"
                        }
                    }
                }

        Returns:
            Status code from the handler (1 for success).
            Returns -1 if the data type is not supported.

        Raises:
            KeyError: If the response structure is invalid or missing required keys.
        """
        if response["assets"]["data"]["type"] == "image/tiff; application=geotiff":
            handler = GeoTiffHandler(response["id"], response["assets"]["data"]["href"])
            return await handler.handle()
        elif response["assets"]["data"]["type"] == "application/geo+json":
            handler = GeoJSONHandler(response["id"], response["assets"]["data"]["href"])
            return await handler.handle()
        else:
            return -1

    async def fetch(self, theme: str) -> int:
        """Fetch all layers for a given theme from STAC catalogs.

        Retrieves all layers associated with the specified theme by:
        1. Getting layer configurations from the themes file
        2. Replacing template variables ({{state}}, {{district}}, {{tehsil}}) with
           actual values (lowercased)
        3. Making HTTP requests to STAC API endpoints
        4. Parsing responses and delegating to appropriate handlers

        The method processes all layers in the theme sequentially and returns the
        status of the last processed layer.

        Args:
            theme: Name of the theme to fetch (e.g., "hydrology", "climate").
                Must match a key in the themes configuration file.

        Returns:
            Status code from the last layer processed:
                - 1: Success
                - -1: Error occurred (connection, request, or parsing error)
                - 0: No layers processed (theme might be empty)

        Raises:
            TypeError: If the theme doesn't exist and get_theme returns None.

        Note:
            This method prints error messages to stdout when exceptions occur.
            Production code should use proper logging instead.

        Example:
            >>> fetcher = LayersFetch("Maharashtra", "Pune", "Haveli")
            >>> status = fetcher.fetch("hydrology")
            >>> print(f"Fetch status: {status}")
            Fetch status: 1
        """
        status: int = 0
        for layer in self._themes.get_theme(theme):
            try:
                response = requests.get(
                    layer["stac_uri"]
                    .replace("{{state}}", self._state.lower())
                    .replace("{{district}}", self._district.lower())
                    .replace("{{tehsil}}", self._tehsil.lower())
                )
                status = await self.parse_response(response.json())
                response.raise_for_status()
            except ConnectionError as e:
                print(f"Connection Error Occurred:\n{e}")
                return -1
            except RequestException as e:
                print(f"Request Exception Occurred:\n{e}")
                return -1
            except Exception as e:
                print(f"Unexpected Error Occurred:\n{e}")
                return -1
        return status
