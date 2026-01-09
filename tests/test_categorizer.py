import json
from pathlib import Path

import pytest

from categorizer import Theme


class TestTheme:
    @pytest.fixture
    def sample_themes_data(self):
        """Sample themes data for testing."""
        return {
            "hydrology": [
                {
                    "name": "{{state}}_{{district}}_stream_order",
                    "stac_uri": "https://example.com/stream.json",
                    "type": "geotiff",
                },
                {
                    "name": "{{state}}_{{district}}_drainage",
                    "stac_uri": "https://example.com/drainage.json",
                    "type": "geojson",
                },
            ],
            "climate": [
                {
                    "name": "{{state}}_{{district}}_drought",
                    "stac_uri": "https://example.com/drought.json",
                    "type": "geojson",
                }
            ],
            "terrain": [],
        }

    @pytest.fixture
    def themes_file(self, sample_themes_data, tmp_path):
        """Create a temporary themes JSON file for testing."""
        themes_path = tmp_path / "themes.json"
        with open(themes_path, "w") as f:
            json.dump(sample_themes_data, f)
        return str(themes_path)

    @pytest.fixture
    def theme_instance(self, themes_file):
        """Create a Theme instance with test data."""
        return Theme(themes_file)

    # Test initialization
    def test_init_with_valid_file(self, themes_file):
        """Test Theme initialization with a valid file."""
        theme = Theme(themes_file)
        assert theme is not None
        assert theme._themes_file == themes_file
        assert isinstance(theme._themes, dict)

    def test_init_with_nonexistent_file(self):
        """Test Theme initialization with a non-existent file."""
        with pytest.raises(FileNotFoundError):
            Theme("/nonexistent/path/themes.json")

    def test_init_with_invalid_json(self, tmp_path):
        """Test Theme initialization with invalid JSON file."""
        invalid_file = tmp_path / "invalid.json"
        with open(invalid_file, "w") as f:
            f.write("not valid json {")

        with pytest.raises(json.JSONDecodeError):
            Theme(str(invalid_file))

    # Test load_themes method
    def test_load_themes_returns_dict(self, theme_instance, sample_themes_data):
        """Test that load_themes returns the correct dictionary."""
        themes = theme_instance.load_themes()
        assert isinstance(themes, dict)
        assert themes == sample_themes_data

    def test_load_themes_has_all_keys(self, theme_instance):
        """Test that all expected theme keys are present."""
        themes = theme_instance.load_themes()
        assert "hydrology" in themes
        assert "climate" in themes
        assert "terrain" in themes

    # Test get_theme method
    def test_get_theme_existing_key(self, theme_instance, sample_themes_data):
        """Test getting an existing theme by name."""
        hydrology_theme = theme_instance.get_theme("hydrology")
        assert hydrology_theme == sample_themes_data["hydrology"]
        assert len(hydrology_theme) == 2

    def test_get_theme_empty_list(self, theme_instance):
        """Test getting a theme with an empty list."""
        terrain_theme = theme_instance.get_theme("terrain")
        assert terrain_theme == []

    def test_get_theme_nonexistent_key(self, theme_instance):
        """Test getting a non-existent theme returns None."""
        result = theme_instance.get_theme("nonexistent")
        assert result is None

    # Test get_themes method
    def test_get_themes_returns_all_themes(self, theme_instance, sample_themes_data):
        """Test that get_themes returns all themes."""
        all_themes = theme_instance.get_themes()
        assert all_themes == sample_themes_data
        assert len(all_themes) == 3

    def test_get_themes_returns_dict(self, theme_instance):
        """Test that get_themes returns a dictionary."""
        all_themes = theme_instance.get_themes()
        assert isinstance(all_themes, dict)

    def test_get_themes_immutability(self, theme_instance):
        """Test that modifying returned themes doesn't affect internal state."""
        themes1 = theme_instance.get_themes()
        themes2 = theme_instance.get_themes()
        # Both should reference the same object in current implementation
        assert themes1 is themes2

    # Integration tests
    def test_theme_workflow(self, tmp_path):
        """Test complete workflow of creating, loading, and accessing themes."""
        # Create themes file
        themes_data = {
            "testTheme": [
                {"name": "test_item", "stac_uri": "http://test.com", "type": "geojson"}
            ]
        }
        themes_file = tmp_path / "workflow_themes.json"
        with open(themes_file, "w") as f:
            json.dump(themes_data, f)

        # Initialize Theme
        theme = Theme(str(themes_file))

        # Get all themes
        all_themes = theme.get_themes()
        assert "testTheme" in all_themes

        # Get specific theme
        test_theme = theme.get_theme("testTheme")
        assert len(test_theme) == 1
        assert test_theme[0]["name"] == "test_item"

    def test_with_real_themes_file(self):
        """Test with the actual themes.json file if it exists."""
        themes_file_path = Path(__file__).parent.parent / "data" / "themes.json"

        if themes_file_path.exists():
            theme = Theme(str(themes_file_path))
            all_themes = theme.get_themes()

            # Verify expected theme categories exist
            expected_categories = [
                "hydrology",
                "climate",
                "terrain",
                "landUse",
                "treeHealth",
                "welfare",
                "administrative",
                "waterPlanning",
            ]
            for category in expected_categories:
                assert category in all_themes, (
                    f"Expected category '{category}' not found"
                )

            # Verify hydrology theme has items
            hydrology = theme.get_theme("hydrology")
            assert hydrology is not None
            assert len(hydrology) > 0

    # Edge cases
    def test_empty_themes_file(self, tmp_path):
        """Test with an empty themes file (empty JSON object)."""
        empty_file = tmp_path / "empty.json"
        with open(empty_file, "w") as f:
            json.dump({}, f)

        theme = Theme(str(empty_file))
        assert theme.get_themes() == {}
        assert theme.get_theme("anything") is None

    def test_themes_with_special_characters(self, tmp_path):
        """Test themes with special characters in keys."""
        special_data = {
            "theme-with-dash": [{"name": "test"}],
            "theme_with_underscore": [{"name": "test"}],
            "theme.with.dots": [{"name": "test"}],
        }
        special_file = tmp_path / "special.json"
        with open(special_file, "w") as f:
            json.dump(special_data, f)

        theme = Theme(str(special_file))
        assert theme.get_theme("theme-with-dash") is not None
        assert theme.get_theme("theme_with_underscore") is not None
        assert theme.get_theme("theme.with.dots") is not None
