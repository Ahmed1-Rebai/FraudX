# syntax=docker/dockerfile:1.7
# One image for every FraudX Python service; the compose `command` selects
# which entrypoint runs.

FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app
# Dependencies first: this layer is reused until the lock file changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim AS runtime
RUN useradd --system --uid 10001 --no-create-home fraudx
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY config ./config
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER fraudx
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]
CMD ["uvicorn", "fraudx.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
