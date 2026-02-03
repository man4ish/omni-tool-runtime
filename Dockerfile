FROM python:3.11-slim

# ---------- system ----------
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ---------- deps ----------
COPY pyproject.toml ./
# or: COPY setup.py ./

RUN pip install --upgrade pip \
 && pip install .

# ---------- code ----------
COPY omni_tool_runtime/ omni_tool_runtime/
COPY tools/ tools/

# ---------- default ----------
# Tools are invoked via command override in Batch (AWS/Azure)
# This keeps the image generic and reusable.
CMD ["python", "-m", "tools.echo_test.run"]
