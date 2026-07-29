FROM python:3.13-slim@sha256:6771159cd4fa5d9bba1258caf0b82e6b73458c694d178ad97c5e925c2d0e1a91

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[runtime]"
COPY docs ./docs
COPY scripts ./scripts
RUN useradd --create-home --uid 10001 thyphon && chown -R thyphon:thyphon /app
USER thyphon

CMD ["uvicorn", "thyphon.api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
