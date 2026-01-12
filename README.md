# CoreStack Geodata Library

A Python library to convert, save, and load geodata from Zarr and Parquet formats.

## Features

- **GeoTIFF Handler**: Load, save, and convert GeoTIFF files to Zarr format
- **GeoJSON Handler**: Process GeoJSON data and convert to Parquet
- **Asynchronous Downloads**: Efficient downloading of geodata with progress tracking
- **Layer Fetching**: Retrieve and categorize geospatial layers from STAC catalogs
- **Theme-based Organization**: Organize geospatial data by themes

## Documentation

Full documentation is available in Sphinx format. To build and view:

```bash
# Install documentation dependencies
uv sync --group docs

# Build HTML documentation
cd docs
make html

# View documentation
open build/html/index.html  # macOS
xdg-open build/html/index.html  # Linux
```

## Examples

### GeoTIFF Handler - Download and Convert to Zarr

The `GeoTiffHandler` downloads GeoTIFF files from a remote URL and automatically converts them to Zarr format.

```python
import asyncio
from src.handlers.geotiff import GeoTiffHandler

async def process_geotiff():
    # Initialize with ID and remote URL
    handler = GeoTiffHandler(
        id="elevation-data",
        url="https://example.com/elevation.tif",
        path="/tmp/geotiff"  # Optional, defaults to /tmp/geotiff
    )

    # Download, load, and save to Zarr - all in one call
    status = await handler.handle()

    if status == 1:
        print("Success! Files saved:")
        print("  - GeoTIFF: /tmp/geotiff/elevation-data.tif")
        print("  - Zarr: /tmp/geotiff/elevation-data.zarr")
    else:
        print("Failed to process GeoTIFF")

# Run the async function
asyncio.run(process_geotiff())
```

### GeoJSON Handler - Download GeoJSON Data

The `GeoJSONHandler` downloads GeoJSON files from a remote URL.

```python
import asyncio
from src.handlers.geojson import GeoJSONHandler

async def process_geojson():
    # Initialize with ID and remote URL
    handler = GeoJSONHandler(
        id="boundaries",
        url="https://example.com/boundaries.geojson",
        temp_path="/tmp/geojson"  # Optional, defaults to /tmp/geojson
    )

    # Download the GeoJSON file
    status = await handler.handle()

    if status == 1:
        print("Success! GeoJSON saved to /tmp/geojson/boundaries.geojson")
    else:
        print("Failed to download GeoJSON")

asyncio.run(process_geojson())
```

### Fetching Layers from STAC Catalogs

The `LayersFetch` class retrieves geospatial layers from STAC catalogs based on themes and geographic filters.

```python
import asyncio
from src.fetch import LayersFetch

async def fetch_layers():
    # Initialize with geographic filters
    fetcher = LayersFetch(
        state="Maharashtra",
        district="Pune",
        tehsil="Haveli",
        themes_file="data/themes.json"
    )

    # Fetch all layers for a specific theme
    status = await fetcher.fetch("hydrology")

    if status == 1:
        print("Successfully fetched all hydrology layers")
    elif status == -1:
        print("Error occurred while fetching layers")
    else:
        print("No layers processed")

asyncio.run(fetch_layers())
```

### Asynchronous File Download with Progress

Use the `Downloader` class for async downloads with progress tracking.

```python
import asyncio
from src.utils.downloader import Downloader

async def download_file():
    # Initialize downloader
    downloader = Downloader(
        url="https://example.com/large-file.tif",
        filepath="/downloads/large-file.tif"
    )

    try:
        # Start async download with progress bar
        await downloader.asyncstart()
        print("Download complete!")
    except ValueError:
        # Fallback to direct download if needed
        await downloader.download()
        if downloader.new_session:
            await downloader.session.close()
    except Exception as e:
        print(f"Download failed: {e}")

asyncio.run(download_file())
```

## Contributing

This project uses `uv` for dependency management, `pre-commit` for code quality, and `ruff` for linting and type-checking.

### Setup

1. **Install uv** (if not installed): <https://github.com/astral-sh/uv>

2. **Install ruff** (if not installed): <https://github.com/astral-sh/ruff>

3. **Setup environment**

   ```bash
   uv sync
   uv run pre-commit install
   ```

### Workflow

1. Create a branch: `git checkout -b feature/your-feature`
2. Make your changes
3. Run tests: `uv run pytest`
4. Commit (pre-commit hooks run automatically)
5. Push and create a PR

### Adding Dependencies

```bash
uv add package-name              # Production dependency
uv add --group dev package-name  # Development dependency
uv add --group docs package-name # Documentation dependency
```
