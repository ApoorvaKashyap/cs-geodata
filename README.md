# CoreStack Geodata Library

This library converts existing geojson and geotiff files to zarr and parquet formats.

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
uv add package-name          # Production dependency
uv add --dev package-name    # Development dependency
```
