# OmniBioAI Tool Runtime — Azure Batch Smoke Test

This document describes a **minimal, deterministic smoke test** for validating that **`omni-tool-runtime`** executes correctly on **Azure Batch** and writes results to **Azure Blob Storage** via `RESULT_URI`.

The goal is to validate the **runtime contract**, not orchestration logic.

---

## Scope (What This Test Proves)

This smoke test validates:

* Azure Batch can pull the container image
* Correct CPU architecture is used (`linux/amd64`)
* `omni_tool_runtime` imports successfully
* Environment variables are injected correctly
* Tool (`echo_test`) executes deterministically
* Results are written to Azure Blob Storage using `RESULT_URI`
* Output JSON can be retrieved and validated

This test **does not** involve `omnibioai-tes` yet.
TES integration is tested separately after runtime validation.

---

## Prerequisites

### Local

* Docker with `buildx`
* Azure CLI (`az`)
* Python ≥ 3.10

### Azure

* Azure Batch account
* Azure Batch pool (Linux, amd64)
* Azure Blob Storage account
* One container for results (e.g. `tes-results`)
* Azure Storage **connection string** available

---

## Image Requirements (CRITICAL)

Azure Batch nodes are **amd64**.

Your dev machine may be **arm64 (aarch64)**.

You **must** publish a **multi-arch image** or an **amd64-only image**.

### Verified Working Image

```
man4ish/omni-tool-runtime:0.1.1-cloud
```

Multi-arch manifest digest (OCI index):

```
sha256:4dc1ac67fdad5677c6e594435bab9dc81a4bb1be6b9f5cccdc10b86bb6e7c45c
```

AMD64 child manifest (used by Azure Batch automatically):

```
linux/amd64
```

---

## Build & Push (Multi-Arch)

```bash
docker buildx create --name omni-builder --use || docker buildx use omni-builder
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t man4ish/omni-tool-runtime:0.1.1-cloud \
  --push \
  .
```

Verify:

```bash
docker buildx imagetools inspect man4ish/omni-tool-runtime:0.1.1-cloud
```

---

## Required Environment Variables

These are injected **per task**.

```bash
TOOL_ID=echo_test
RUN_ID=<unique-run-id>
INPUTS_JSON={"text":"hello from az batch probe"}
RESOURCES_JSON={}
RESULT_URI=azureblob://<storage-account>/<container>/tes-runs/<RUN_ID>/tools/echo_test/results.json

AZURE_AUTH=connection_string
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=...
```

> Note: **Never hardcode secrets** into task templates permanently.

---

## Azure Batch Job & Pool (Assumed Existing)

Example verified working values:

```bash
POOL_ID=tesp-container
JOB_ID=tes-job-01
```

Verify:

```bash
az batch pool list -o table
az batch job list -o table
```

---

## Smoke Test Task Definition

Minimal task JSON (`task.envprobe.json`):

```json
{
  "id": "tes-echo-<RUN_ID>",
  "commandLine": "/bin/sh -lc 'python -m tools.echo_test.run'",
  "containerSettings": {
    "imageName": "man4ish/omni-tool-runtime@sha256:4dc1ac67fdad5677c6e594435bab9dc81a4bb1be6b9f5cccdc10b86bb6e7c45c"
  },
  "environmentSettings": [
    {"name": "TOOL_ID", "value": "echo_test"},
    {"name": "RUN_ID", "value": "<RUN_ID>"},
    {"name": "INPUTS_JSON", "value": "{\"text\":\"hello from az batch probe\"}"},
    {"name": "RESOURCES_JSON", "value": "{}"},
    {
      "name": "RESULT_URI",
      "value": "azureblob://<account>/<container>/tes-runs/<RUN_ID>/tools/echo_test/results.json"
    },
    {"name": "AZURE_AUTH", "value": "connection_string"},
    {
      "name": "AZURE_STORAGE_CONNECTION_STRING",
      "value": "<INJECT_AT_SUBMIT_TIME>"
    }
  ]
}
```

---

## Submit the Task

```bash
export JOB_ID="tes-job-01"
export RUN_ID="azbatch-echo-$(date +%Y%m%d-%H%M%S)"
export TASK_ID="tes-echo-${RUN_ID}"

# Update task id and RUN_ID dynamically
python - <<PY
import json
p="task.envprobe.json"
d=json.load(open(p))
d["id"]="${TASK_ID}"
for e in d["environmentSettings"]:
    if e["name"]=="RUN_ID":
        e["value"]="${RUN_ID}"
    if e["name"]=="RESULT_URI":
        e["value"]="azureblob://<account>/<container>/tes-runs/${RUN_ID}/tools/echo_test/results.json"
json.dump(d, open(p,"w"), indent=2)
PY

az batch task create \
  --job-id "$JOB_ID" \
  --json-file task.envprobe.json
```

---

## Check Task Status

```bash
az batch task show \
  --job-id "$JOB_ID" \
  --task-id "$TASK_ID" \
  --query "{state:state, exit:executionInfo.exitCode, result:executionInfo.result}" \
  -o jsonc
```

Expected:

```json
{
  "state": "completed",
  "exit": 0,
  "result": "success"
}
```

---

## Fetch Logs

```bash
az batch task file download \
  --job-id "$JOB_ID" \
  --task-id "$TASK_ID" \
  --file-path stdout.txt \
  --destination ./stdout.txt \
  --output none

az batch task file download \
  --job-id "$JOB_ID" \
  --task-id "$TASK_ID" \
  --file-path stderr.txt \
  --destination ./stderr.txt \
  --output none
```

Expected `stdout.txt`:

```json
{
  "ok": true,
  "tool_id": "echo_test",
  "run_id": "...",
  "results": {
    "echo": "hello from az batch probe"
  }
}
```

`stderr.txt` should be empty.

---

## Verify Result Blob

```bash
az storage blob download \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name "<container>" \
  --name "tes-runs/${RUN_ID}/tools/echo_test/results.json" \
  --file ./results.json \
  --output none

python -m json.tool ./results.json
```

Expected:

```json
{
  "ok": true,
  "tool_id": "echo_test",
  "run_id": "...",
  "results": {
    "echo": "hello from az batch probe"
  }
}
```

---

## Known Pitfalls (Lessons Learned)

### 1. `exec format error`

Cause: arm64 image on amd64 Batch nodes
Fix: multi-arch build (`buildx`)

### 2. Blob auth errors after key rotation

Cause: stale connection string embedded in old task JSON
Fix: always inject secrets at submit time

### 3. Batch exit code ≠ tool success

Even if stdout shows valid JSON, **non-zero exit code fails the task**
Ensure tool exits cleanly after upload

---

## Current Status

| Platform                  | Runtime Smoke Test |
| ------------------------- | ------------------ |
| AWS Batch (Fargate)       | ✅ Passed           |
| Azure Batch               | ✅ Passed           |
| Kubernetes (Minikube)     | ⏳ Pending          |
| omnibioai-tes integration | ⏳ Pending          |

---

## Next Steps

1. Run the same test via `omnibioai-tes` Azure adapter
2. Implement K8s (Minikube) smoke test
3. Freeze `omni-tool-runtime` **v1.0**
4. Add CI smoke tests per platform
