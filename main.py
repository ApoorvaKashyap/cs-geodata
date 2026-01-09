from src.categorizer import Theme


def main() -> None:
    theme_manager = Theme("data/themes.json")
    themes = theme_manager.get_themes()
    print("Available Themes:")
    for theme_name in themes:
        print(f"- {theme_name}:", end=":")
        theme = theme_manager.get_theme(theme_name)
        if theme:
            print(f" {theme}")
        else:
            print(" Theme not found.")


if __name__ == "__main__":
    main()
