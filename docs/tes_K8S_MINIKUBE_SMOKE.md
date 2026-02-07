# OmniBioAI Tool Runtime — Kubernetes (Minikube) Smoke Test

This document describes a **minimal, deterministic smoke test** for validating that **`omni-tool-runtime`** executes correctly on **Kubernetes (Minikube)** and writes results to an external object store via `RESULT_URI`.

This validates the **runtime execution contract only** — not orchestration, scheduling, or TES logic.

---

## Scope (What This Test Proves)

This smoke test validates:

* Kubernetes can run the container image successfully
* Correct CPU architecture is selected (multi-arch image)
* `omni_tool_runtime` imports successfully
* Environment variables and secrets are injected correctly
* Tool (`echo_test`) executes deterministically
* Results are written to external object storage via `RESULT_URI`
* Logs, exit codes, and termination state reflect success correctly

This test is **standalone** and does **not** require `omnibioai-tes`.

---

## Prerequisites

### Local

* Docker
* kubectl
* Minikube
* Python ≥ 3.10

### Kubernetes

* Single-node Minikube cluster
* Internet access from pods (default Minikube networking is sufficient)

---

## Start Minikube

```bash
minikube start --driver=docker
kubectl get nodes -o wide
```

Expected:

```
NAME       STATUS   ROLES           AGE   VERSION
minikube   Ready    control-plane   ...   v1.xx.x
```

---

## Image Requirements

Use the **same image validated for AWS Batch and Azure Batch**.

### Verified Working Image

```
man4ish/omni-tool-runtime:0.1.1-cloud
```

This is a **multi-architecture image**. Kubernetes automatically selects the correct platform (e.g., `linux/arm64` on Apple Silicon / ARM hosts).

If running fully offline or to avoid pulling from Docker Hub:

```bash
minikube image load man4ish/omni-tool-runtime:0.1.1-cloud
```

Verify:

```bash
minikube image ls | grep omni-tool-runtime
```

---

## Secrets: Storage Credentials

Inject storage credentials via a Kubernetes Secret.

### Create secret (Azure Blob example)

```bash
kubectl create secret generic omnibioai-storage-secret \
  --from-file=AZURE_STORAGE_CONNECTION_STRING=$HOME/.omnibioai/secrets/azure_storage_connection_string.txt
```

Verify:

```bash
kubectl describe secret omnibioai-storage-secret
```

> The secret age showing `0s` immediately after creation is expected and does **not** indicate a hang.

---

## Smoke Test Job (`echo_test`)

Create `echo-smoke-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: omni-runtime-echo-smoke
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: echo-test
          image: man4ish/omni-tool-runtime:0.1.1-cloud
          imagePullPolicy: IfNotPresent
          command:
            - /bin/sh
            - -lc
            - python -m tools.echo_test.run
          env:
            - name: TOOL_ID
              value: echo_test
            - name: RUN_ID
              value: k8s-minikube-smoke
            - name: INPUTS_JSON
              value: '{"text":"hello from k8s minikube"}'
            - name: RESOURCES_JSON
              value: '{}'
            - name: RESULT_URI
              value: azureblob://<account>/<container>/tes-runs/k8s-minikube-smoke/tools/echo_test/results.json
            - name: AZURE_AUTH
              value: connection_string
            - name: AZURE_STORAGE_CONNECTION_STRING
              valueFrom:
                secretKeyRef:
                  name: omnibioai-storage-secret
                  key: AZURE_STORAGE_CONNECTION_STRING
```

> `RESULT_URI` may also be `s3://…` or `file:///…` depending on backend.

---

## Run the Job

```bash
kubectl apply -f echo-smoke-job.yaml
```

Observe:

```bash
kubectl get jobs
kubectl get pods --selector=job-name=omni-runtime-echo-smoke
```

Expected:

```
NAME                      STATUS     COMPLETIONS   DURATION
omni-runtime-echo-smoke   Complete   1/1           <few seconds>
```

Pod state:

```
READY   STATUS      RESTARTS
0/1     Completed   0
```

> `READY 0/1` is **expected** for completed Job pods.

---

## Check Pod Logs

```bash
POD=$(kubectl get pods -l job-name=omni-runtime-echo-smoke -o jsonpath='{.items[0].metadata.name}')
kubectl logs "$POD"
```

Expected output:

```json
{
  "ok": true,
  "tool_id": "echo_test",
  "run_id": "k8s-minikube-smoke",
  "results": {
    "echo": "hello from k8s minikube"
  }
}
```

Followed by storage upload confirmation, for example:

```
[azureblob] upload container=tes-results blob=tes-runs/k8s-minikube-smoke/tools/echo_test/results.json
```

---

## Verify Job Exit Code

```bash
kubectl get pod "$POD" -o jsonpath='{.status.containerStatuses[0].state.terminated}'; echo
```

Expected:

```json
{
  "exitCode": 0,
  "reason": "Completed"
}
```

---

## Verify Result Artifact

### Azure Blob example

```bash
az storage blob download \
  --connection-string "$(cat $HOME/.omnibioai/secrets/azure_storage_connection_string.txt)" \
  --container-name "<container>" \
  --name "tes-runs/k8s-minikube-smoke/tools/echo_test/results.json" \
  --file ./results.json \
  --output none

python -m json.tool ./results.json
```

Expected:

```json
{
  "ok": true,
  "tool_id": "echo_test",
  "run_id": "k8s-minikube-smoke",
  "results": {
    "echo": "hello from k8s minikube"
  }
}
```

---

## Cleanup

```bash
kubectl delete job omni-runtime-echo-smoke
kubectl delete secret omnibioai-storage-secret
```

(Optional)

```bash
minikube stop
```

---

## Known Pitfalls

### 1. Image pull fails

* Ensure Minikube has internet access
* Or preload the image:

```bash
minikube image load man4ish/omni-tool-runtime:0.1.1-cloud
```

---

### 2. Secret not visible inside pod

* Confirm secret key name matches exactly
* Use `kubectl describe pod` to inspect injected env vars

---

### 3. Job completes but artifact missing

* Verify `RESULT_URI`
* Confirm storage credentials
* Ensure external network access from Minikube

---

## Current Runtime Smoke Status

| Platform                  | Status    |
| ------------------------- | --------- |
| AWS Batch                 | ✅ Passed  |
| Azure Batch               | ✅ Passed  |
| Kubernetes (Minikube)     | ✅ Passed  |
| omnibioai-tes integration | ⏳ Pending |

---

## Next Steps

1. Promote this Job to a **formal K8s smoke test**
2. Implement **TES Kubernetes adapter**
3. Run TES → K8s end-to-end execution
4. Freeze `omni-tool-runtime` **v1.0**
