"""Theme management module for categorizing and organizing geo data themes.

This module provides functionality to load, access, and manage themes from a JSON
configuration file. Themes are used to categorize different types of geographic data.
"""

import json


class Theme:
    def __init__(self, themes_file: str) -> None:
        self._themes_file = themes_file
        self._themes = self.load_themes()

    def load_themes(self) -> dict:
        """Load themes from the JSON configuration file.

        Returns:
            A dictionary containing the theme data loaded from the JSON file.

        Raises:
            FileNotFoundError: If the themes file does not exist.
            json.JSONDecodeError: If the themes file contains invalid JSON.
        """
        with open(self._themes_file) as f:
            return json.load(f)

    def get_theme(self, name: str) -> list | None:
        """Retrieve a specific theme by name.

        Args:
            name: The name of the theme to retrieve.

        Returns:
            A list containing the theme data if found, None otherwise.
        """
        return self._themes.get(name)

    def get_themes(self) -> dict:
        """Retrieve all loaded themes.

        Returns:
            A dictionary containing all theme data.
        """
        return self._themes
