FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BMM_BIND_HOST=127.0.0.1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY configs ./configs
COPY scripts ./scripts

RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev

EXPOSE 8000
CMD ["uv", "run", "binance-market-monitor", "monitor", "--config", "configs/config.example.yaml"]
