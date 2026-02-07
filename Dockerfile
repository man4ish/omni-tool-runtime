# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy only necessary files for packaging
COPY . /app

# Install dependencies
RUN echo "USING Dockerfile.cloud" \
 && pip install --upgrade pip \
 && pip install --no-cache-dir . \
 && pip install --no-cache-dir boto3 azure-identity azure-storage-blob \
 && python -c "import omni_tool_runtime; print('omni_tool_runtime import OK')"

CMD ["python", "-m", "tools.echo_test.run"]
