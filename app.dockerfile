# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim
WORKDIR /app

ENV UV_NO_DEV=1

WORKDIR /app

RUN groupadd --gid 1000 appgroup && useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser
RUN chown -R appuser:appgroup /app
USER appuser

COPY pyproject.toml uv.lock /app/

RUN uv sync --locked

COPY --exclude=.venv --exclude=.git --exclude=.env . /app/


CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
