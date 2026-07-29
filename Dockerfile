FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[runtime]"
COPY docs ./docs
RUN useradd --create-home --uid 10001 thyphon && chown -R thyphon:thyphon /app
USER thyphon

CMD ["uvicorn", "thyphon.api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
