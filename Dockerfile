FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
WORKDIR /service
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --python /usr/local/bin/python
COPY app ./app
COPY alembic.ini ./
COPY migrations ./migrations
ENV PATH="/service/.venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN useradd --create-home app
USER app
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
