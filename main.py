import asyncio

from src.fetch import LayersFetch


async def main() -> None:
    layers = LayersFetch("Jharkhand", "Godda", "Sundarpahari", "data/themes.json")
    await layers.fetch("hydrology")


if __name__ == "__main__":
    asyncio.run(main())
