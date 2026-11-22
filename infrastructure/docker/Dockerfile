FROM python:3.15.2-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /uvx /usr/local/bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_NO_CACHE=1 \
    PATH="/app/.venv/bin:$PATH" \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1
WORKDIR /app

# The same immutable application image is independently deployed with each service command.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY models ./models
RUN uv sync --frozen --no-dev --no-editable \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app \
    && chown -R app:app /app
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "feedback_platform.ingestion:app", "--host", "0.0.0.0", "--port", "8000"]
