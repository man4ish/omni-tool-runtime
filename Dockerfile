# tool-runtime is standalone — DO NOT use omnibioai-base
# It needs Java, Nextflow, Apptainer — unrelated to ML stack
FROM --platform=linux/amd64 python:3.11-slim-bookworm
LABEL org.opencontainers.image.source=https://github.com/man4ish/omnibioai
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl git ca-certificates tar wget \
    openjdk-17-jre-headless \
    squashfs-tools fuse2fs fuse libfuse3-3 \
    uidmap libseccomp2 libglib2.0-0 fakeroot \
    docker.io fastqc \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o apptainer.deb \
    "https://github.com/apptainer/apptainer/releases/download/v1.3.4/apptainer_1.3.4_amd64.deb" \
    && apt-get update -qq \
    && dpkg -i apptainer.deb || apt-get install -f -y \
    && rm apptainer.deb && rm -rf /var/lib/apt/lists/*

RUN curl -s https://get.nextflow.io | bash \
    && mv nextflow /usr/local/bin/ \
    && chmod +x /usr/local/bin/nextflow

COPY pyproject.toml .
RUN pip install --no-cache-dir . \
    && pip install --no-cache-dir boto3 azure-identity azure-storage-blob

COPY . .
CMD ["python", "-m", "tools.echo_test.run"]