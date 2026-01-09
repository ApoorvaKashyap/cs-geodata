import json


class Theme:
    def __init__(self, themes_file: str) -> None:
        self._themes_file = themes_file
        self._themes = self.load_themes()

    def load_themes(self) -> dict:
        with open(self._themes_file) as f:
            return json.load(f)

    def get_theme(self, name: str) -> list | None:
        return self._themes.get(name)

    def get_themes(self) -> dict:
        return self._themes
