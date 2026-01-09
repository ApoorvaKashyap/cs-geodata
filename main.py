from src.fetch import LayersFetch


def main() -> None:
    layers = LayersFetch("Jharkhand", "Godda", "Sundarpahari", "data/themes.json")
    layers.fetch("treeHealth")


if __name__ == "__main__":
    main()
