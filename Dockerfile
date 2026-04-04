# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- OS dependencies ----
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      curl \
      git \
      ca-certificates \
      openjdk-17-jre-headless \
      tar \
      wget \
      squashfs-tools \
      fuse2fs \
      fuse \
      uidmap \
      libseccomp-dev \
      libglib2.0-dev \
      pkg-config \
    && rm -rf /var/lib/apt/lists/*

# ---- Install Go ----
RUN ARCH=$(dpkg --print-architecture) \
    && curl -fsSL \
      "https://go.dev/dl/go1.21.0.linux-${ARCH}.tar.gz" \
      -o go.tar.gz \
    && tar -C /usr/local -xzf go.tar.gz \
    && rm go.tar.gz

ENV PATH="/usr/local/go/bin:$PATH"

# ---- Build Apptainer from source ----
RUN git clone --depth 1 --branch v1.4.5 \
      https://github.com/apptainer/apptainer.git /tmp/apptainer \
    && cd /tmp/apptainer \
    && ./mconfig --without-suid \
    && make -C builddir \
    && make -C builddir install \
    && rm -rf /tmp/apptainer \
    && apptainer --version

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

CMD ["python", "-m", "tools.echo_test.run"]
