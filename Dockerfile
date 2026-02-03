# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml README.md /app/
COPY omni_tool_runtime/ /app/omni_tool_runtime/
COPY tools/ /app/tools/

RUN echo "USING Dockerfile.cloud" \
 && pip install --upgrade pip \
 && pip install --no-cache-dir . \
 && pip install --no-cache-dir boto3 azure-identity azure-storage-blob

CMD ["python", "-m", "tools.echo_test.run"]
