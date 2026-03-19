# tools/workflow_runner/run.py
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or default)


def _load_json_env(name: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = _env(name, "")
    if not raw:
        return default or {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else (default or {})
    except Exception:
        return default or {}


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _normalize_result_uri(result_uri: str) -> tuple[str, str]:
    """
    Returns (results_uri, outputs_uri)

    - If RESULT_URI ends with .json -> treat as results.json location
    - Else treat as a prefix/folder and append /results.json and /outputs.json
    """
    s = (result_uri or "").rstrip()
    if not s:
        raise RuntimeError("RESULT_URI is required")

    if s.endswith(".json"):
        # Put outputs.json next to results.json
        base = s.rsplit("/", 1)[0]
        return s, f"{base}/outputs.json"

    s = s.rstrip("/")
    return f"{s}/results.json", f"{s}/outputs.json"


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))


def _upload_uri(uri: str, data: bytes, content_type: str = "application/json") -> None:
    """
    Minimal multi-backend uploader:
      - s3://bucket/key
      - azureblob://account/container/path
      - file:///path or plain /path (local)
    """
    u = urlparse(uri)

    if u.scheme == "s3":
        import boto3  # type: ignore

        bucket = u.netloc
        key = u.path.lstrip("/")
        if not bucket or not key:
            raise RuntimeError(f"Bad S3 URI: {uri}")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        return

    if u.scheme == "azureblob":
        # Auth: connection string (typical for dev) or managed identity if you extend later
        from azure.storage.blob import BlobServiceClient  # type: ignore

        # account = u.netloc
        path = u.path.lstrip("/")
        if "/" not in path:
            raise RuntimeError(f"Bad azureblob URI (need container/blob): {uri}")
        container, blob = path.split("/", 1)

        # Prefer standard env var
        cs = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or os.getenv("OMNI_TOOL_RUNTIME_AZURE_CONNECTION_STRING") or ""
        if not cs:
            raise RuntimeError("Missing AZURE_STORAGE_CONNECTION_STRING for azureblob upload")

        svc = BlobServiceClient.from_connection_string(cs)
        bc = svc.get_blob_client(container=container, blob=blob)
        bc.upload_blob(data, overwrite=True, content_settings=None)
        return

    # local path
    if u.scheme in ("", "file"):
        dst = Path(u.path if u.scheme == "file" else uri)
        _ensure_dir(dst.parent)
        dst.write_bytes(data)
        return

    raise RuntimeError(f"Unsupported RESULT_URI scheme: {u.scheme} ({uri})")


def _run_command(cmd: list[str] | str, cwd: Path, env: dict[str, str]) -> int:
    if isinstance(cmd, str):
        p = subprocess.run(cmd, shell=True, cwd=str(cwd), env=env)
        return int(p.returncode)
    p = subprocess.run(cmd, cwd=str(cwd), env=env)
    return int(p.returncode)


def main() -> int:
    tool_id = _env("TOOL_ID", "workflow_runner")
    run_id = _env("RUN_ID", "")
    inputs = _load_json_env("INPUTS_JSON", {})
    resources = _load_json_env("RESOURCES_JSON", {})

    if not run_id:
        raise RuntimeError("RUN_ID is required")

    result_uri_raw = _env("RESULT_URI", "")
    results_uri, outputs_uri = _normalize_result_uri(result_uri_raw)

    # Your stated convention
    work_root = Path(_env("WORK_ROOT", "work"))
    exec_root = work_root / "workflow_runs_exec" / run_id
    _ensure_dir(exec_root)

    # Optional: copy workflow bundle/material into exec_root if user passes a local path
    # (kept minimal and safe)
    local_bundle = inputs.get("local_bundle_path")
    if isinstance(local_bundle, str) and local_bundle.strip():
        src = Path(local_bundle).expanduser().resolve()
        if src.exists() and src.is_dir():
            # copytree into a subdir to avoid clobbering exec_root
            dst = exec_root / "bundle"
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)

    # Decide what to run (keep flexible)
    # Preferred: explicit command supplied by Workflow Service / plugin
    cmd = inputs.get("command")
    if cmd is None:
        # Fallback: engine + workflow + params pattern (still generic)
        engine = str(inputs.get("engine") or "").lower()
        wf = inputs.get("workflow") or inputs.get("workflow_path") or inputs.get("workflow_uri")
        if engine and wf:
            # very generic placeholders; you can tighten later
            if engine == "nextflow":
                cmd = ["nextflow", "run", str(wf)]
            elif engine == "snakemake":
                cmd = ["snakemake", "-s", str(wf)]
            elif engine == "cwl":
                cmd = ["cwltool", str(wf)]
            else:
                cmd = str(inputs.get("command_str") or "")
        else:
            cmd = str(inputs.get("command_str") or "")

    # environment passed into workflow
    child_env = os.environ.copy()
    child_env["OMNIBIOAI_WORKFLOW_RUN_ID"] = run_id
    child_env["OMNIBIOAI_WORKFLOW_EXEC_ROOT"] = str(exec_root)
    # optionally pass resources to the workflow runner
    child_env["OMNIBIOAI_RESOURCES_JSON"] = json.dumps(resources)

    rc = 0
    err: str | None = None
    try:
        rc = _run_command(cmd, cwd=exec_root, env=child_env)  # type: ignore[arg-type]
    except Exception as e:
        rc = 1
        err = str(e)

    # Expect outputs.json per your design
    outputs_path = exec_root / "outputs.json"
    outputs_obj: dict[str, Any]
    if outputs_path.exists():
        try:
            outputs_obj = json.loads(outputs_path.read_text())
            if not isinstance(outputs_obj, dict):
                outputs_obj = {"_raw": outputs_obj}
        except Exception as e:
            outputs_obj = {"ok": False, "error": f"outputs.json parse failed: {e}"}
    else:
        outputs_obj = {"ok": (rc == 0), "note": "outputs.json not produced by runner"}

    # Compose TES-facing results.json (small + stable)
    results_obj = {
        "ok": (rc == 0),
        "tool_id": tool_id,
        "run_id": run_id,
        "engine": inputs.get("engine"),
        "exec_root": str(exec_root),
        "results_uri": results_uri,
        "outputs_uri": outputs_uri,
        "outputs": outputs_obj if len(json.dumps(outputs_obj)) < 200_000 else {"note": "outputs too large; see outputs_uri"},
    }
    if err:
        results_obj["error"] = err
    if rc != 0:
        results_obj["exit_code"] = rc

    # Upload outputs.json (best-effort), then results.json (must)
    try:
        _upload_uri(outputs_uri, json.dumps(outputs_obj, indent=2).encode("utf-8"))
    except Exception as e:
        # don’t fail run just because outputs side-upload failed
        results_obj["outputs_upload_error"] = str(e)

    _upload_uri(results_uri, json.dumps(results_obj, indent=2).encode("utf-8"))

    return 0 if rc == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
