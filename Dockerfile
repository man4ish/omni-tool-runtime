# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- OS dependencies for Nextflow ----
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      curl \
      git \
      ca-certificates \
      openjdk-21-jre-headless \
      tar \
    && rm -rf /var/lib/apt/lists/*

# ---- Install Nextflow ----
RUN curl -s https://get.nextflow.io | bash \
    && mv nextflow /usr/local/bin/nextflow \
    && chmod +x /usr/local/bin/nextflow \
    && nextflow -version

COPY . /app

RUN pip install --upgrade pip \
 && pip install --no-cache-dir . \
 && pip install --no-cache-dir boto3 azure-identity azure-storage-blob \
 && python -c "import omni_tool_runtime; print('omni_tool_runtime import OK')"

# Generic container; Batch/TES overrides the command
CMD ["python", "-m", "tools.echo_test.run"]
