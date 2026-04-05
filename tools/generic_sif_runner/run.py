# tools/generic_sif_runner/run.py
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from omni_tool_runtime.upload_result import upload_to_result_uri


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or default)


def _resolve_env_refs(s: str) -> str:
    """Expand ${VAR} and $VAR in strings."""
    def _replace(m):
        var = m.group(1) or m.group(2)
        return os.environ.get(var, m.group(0))
    return re.sub(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)", _replace, s)


def _fetch_sif(sif_uri: str, cache_dir: Path) -> Path:
    """
    Resolve SIF image to a local path.
    Supports:
      /local/path/fastqc_arm64.sif        — Slurm/DGX
      s3://bucket/fastqc_arm64.sif        — AWS
      azureblob://account/container/name  — Azure
    """
    uri = _resolve_env_refs(sif_uri)

    # ✅ If local path doesn't exist but SIF_BASE is set
    # → rewrite to S3/Azure URI automatically
    sif_base = _env("SIF_BASE", "").strip()
    if sif_base and not uri.startswith("s3://") and not uri.startswith("azureblob://"):
        if not Path(uri).exists():
            # Extract just the filename
            sif_name = Path(uri).name
            uri = f"{sif_base.rstrip('/')}/{sif_name}"
            print(f"[generic_sif_runner] local SIF not found, using: {uri}")

    # Local path
    if not uri.startswith("s3://") and not uri.startswith("azureblob://"):
        p = Path(uri)
        if not p.exists():
            raise FileNotFoundError(f"SIF not found: {p}")
        return p

    # Check local cache first
    sif_name = Path(uri.split("/")[-1]).name
    local_path = cache_dir / sif_name
    if local_path.exists():
        size_mb = local_path.stat().st_size / 1e6
        print(f"[sif_cache] hit: {local_path} ({size_mb:.0f}MB)")
        return local_path

    cache_dir.mkdir(parents=True, exist_ok=True)
    print(f"[sif_cache] downloading {uri} → {local_path}")

    if uri.startswith("s3://"):
        _fetch_from_s3(uri, local_path)

    elif uri.startswith("azureblob://"):
        _fetch_from_azure(uri, local_path)

    size_mb = local_path.stat().st_size / 1e6
    print(f"[sif_cache] ready: {local_path} ({size_mb:.0f}MB)")
    return local_path


def _fetch_from_s3(uri: str, dest: Path) -> None:
    # Use boto3 directly (no aws CLI needed in container)
    try:
        import boto3
        from urllib.parse import urlparse
        u = urlparse(uri)
        bucket = u.netloc
        key = u.path.lstrip("/")
        print(f"[sif_cache] boto3 download: s3://{bucket}/{key} → {dest}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        boto3.client("s3").download_file(bucket, key, str(dest))
        print(f"[sif_cache] download complete: {dest.stat().st_size / 1e6:.0f}MB")
    except Exception as e:
        raise RuntimeError(f"S3 download failed for {uri}: {e}")


def _fetch_from_azure(uri: str, dest: Path) -> None:
    from urllib.parse import urlparse
    u = urlparse(uri)
    account = u.netloc
    path = u.path.lstrip("/")
    container, blob = path.split("/", 1)

    auth = _env("AZURE_AUTH", "managed_identity")
    cs   = _env("AZURE_STORAGE_CONNECTION_STRING") or None

    try:
        from azure.storage.blob import BlobServiceClient
        if auth == "connection_string" and cs:
            svc = BlobServiceClient.from_connection_string(cs)
        else:
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
            svc  = BlobServiceClient(
                account_url=f"https://{account}.blob.core.windows.net",
                credential=cred,
            )
        bc = svc.get_blob_client(container=container, blob=blob)
        dest.write_bytes(bc.download_blob().readall())
    except Exception as e:
        raise RuntimeError(f"Azure Blob download failed for {uri}: {e}")


def _load_tool_def() -> Dict[str, Any]:
    # Option 1 — inline JSON injected by TES adapter
    raw = _env("TOOL_DEF_JSON")
    if raw:
        return json.loads(raw)

    # Option 2 — path to JSON file
    path = _env("TOOL_DEF_PATH")
    if path and Path(path).exists():
        return json.loads(Path(path).read_text())

    # Option 3 — fetch from live TES API
    tes_url = _env("TES_URL", "http://127.0.0.1:8081")
    tool_id = _env("TOOL_ID")
    if tes_url and tool_id:
        import urllib.request
        with urllib.request.urlopen(f"{tes_url}/api/tools") as r:
            for t in json.loads(r.read()):
                if t.get("tool_id") == tool_id:
                    return t

    raise RuntimeError(
        "Cannot load tool definition. "
        "Set TOOL_DEF_JSON, TOOL_DEF_PATH, or TES_URL."
    )


def _resolve_command(
    template_parts: list[str],
    inputs: Dict[str, Any],
    work_dir: str,
    resources: Dict[str, Any] = None,
) -> list[str]:
    # Build context with defaults for common placeholders
    resources = resources or {}
    context = {
        "work_dir":  work_dir,
        "threads":   str(resources.get("cpu", 1)),
        "cpu":       str(resources.get("cpu", 1)),
        "ram_gb":    str(resources.get("ram_gb", 4)),
        "memory":    str(resources.get("ram_gb", 4)),
    }
    context.update({k: str(v) for k, v in inputs.items()})

    resolved = []
    for part in template_parts:
        try:
            resolved.append(_resolve_env_refs(part.format(**context)))
        except KeyError as e:
            raise RuntimeError(
                f"Missing input for command placeholder: {e}"
            )
    return resolved


def _collect_outputs(
    work_dir: Path,
    output_patterns: list[Dict[str, Any]],
) -> Dict[str, Any]:
    import glob
    outputs = {}
    for spec in output_patterns:
        name    = spec.get("name", "output")
        pattern = spec.get("pattern", "*")
        matches = sorted(glob.glob(str(work_dir / pattern)))
        if len(matches) == 1:
            outputs[name] = matches[0]
        elif len(matches) > 1:
            outputs[name] = matches
        else:
            outputs[name] = None
            print(f"[generic_sif_runner] WARNING: no files matched pattern '{pattern}' for output '{name}'")
    return outputs


def main() -> int:
    tool_id    = _env("TOOL_ID")
    run_id     = _env("RUN_ID")
    result_uri = _env("RESULT_URI", "").strip()

    try:
        inputs    = json.loads(_env("INPUTS_JSON",    "{}"))
        resources = json.loads(_env("RESOURCES_JSON", "{}"))
    except Exception as e:
        print(f"ERROR: bad INPUTS_JSON / RESOURCES_JSON: {e}", file=sys.stderr)
        return 2

    # ── Load tool definition ──────────────────────────────────
    try:
        tool_def = _load_tool_def()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    slurm_def       = tool_def.get("slurm") or {}
    sif_uri         = _resolve_env_refs(slurm_def.get("image", ""))
    cmd_template    = slurm_def.get("command", [])
    output_patterns = slurm_def.get("outputs", [])

    if not sif_uri:
        print(f"ERROR: tool {tool_id} has no slurm.image", file=sys.stderr)
        return 2

    # ── Work dir ──────────────────────────────────────────────
    work_dir = Path(
        _env("WORK_DIR") or
        tempfile.mkdtemp(prefix=f"tes_{tool_id}_{run_id}_")
    )
    work_dir.mkdir(parents=True, exist_ok=True)
    print(f"[generic_sif_runner] tool={tool_id} run_id={run_id}")
    print(f"[generic_sif_runner] work_dir={work_dir}")

    # ── Fetch SIF (local or cloud) ────────────────────────────
    cache_dir = Path(_env("SIF_CACHE_DIR", "/tmp/omnibioai_sif_cache"))
    docker_image = slurm_def.get("docker_image", "")
    try:
        local_sif = _fetch_sif(sif_uri, cache_dir)
        use_docker = False
    except Exception as e:
        if docker_image:
            print(f"[generic_sif_runner] SIF unavailable, using Docker: {docker_image}")
            use_docker = True
            local_sif = None
        else:
            print(f"ERROR: SIF fetch failed: {e}", file=sys.stderr)
            return 2

    # ── Resolve command template ──────────────────────────────
    try:
        resolved_cmd = _resolve_command(cmd_template, inputs, str(work_dir), resources)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    # ── Run via Docker or Singularity ────────────────────────
    cpu = int(resources.get("cpu", 1) or 1)
    # Force Docker if docker_image set and SIF is wrong arch
    sif_arch = str(local_sif).split("_")[-1].replace(".sif","") if local_sif else ""
    import platform
    host_arch = platform.machine()  # x86_64 or aarch64
    arch_mismatch = (
        (sif_arch == "arm64" and host_arch == "x86_64") or
        (sif_arch == "amd64" and host_arch == "aarch64")
    )
    if arch_mismatch and docker_image:
        print(f"[generic_sif_runner] arch mismatch ({sif_arch} on {host_arch}), using Docker: {docker_image}")
        use_docker = True

    if use_docker:
        singularity_cmd = [
            "docker", "run", "--rm",
            "-v", f"{work_dir}:{work_dir}",
            "-v", f"{work_dir}:/tmp",
            "-w", str(work_dir),
            docker_image,
        ] + resolved_cmd
    else:
        singularity_cmd = [
            "singularity", "exec",
            "--no-home",
            "--writable-tmpfs",
            "--bind", f"{work_dir}:{work_dir}",
            "--bind", f"{work_dir}:/tmp",
            str(local_sif),
        ] + resolved_cmd

    # Bind any input file paths that exist on host
    for v in inputs.values():
        if isinstance(v, str) and Path(v).exists():
            parent = str(Path(v).parent)
            singularity_cmd.insert(
                singularity_cmd.index(str(local_sif)),
                "--bind"
            )
            singularity_cmd.insert(
                singularity_cmd.index(str(local_sif)),
                f"{parent}:{parent}:ro"
            )

    print(f"[generic_sif_runner] cmd: {' '.join(singularity_cmd)}")

    proc = subprocess.run(
        singularity_cmd,
        capture_output=True,
        text=True,
        env={**os.environ, "OMP_NUM_THREADS": str(cpu)},
    )

    stdout   = proc.stdout or ""
    stderr   = proc.stderr or ""
    ok       = proc.returncode == 0

    print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)

    # ── Collect outputs ───────────────────────────────────────
    outputs = _collect_outputs(work_dir, output_patterns)

    # ── Build result ──────────────────────────────────────────
    result_obj = {
        "ok":          ok,
        "tool_id":     tool_id,
        "run_id":      run_id,
        "exit_code":   proc.returncode,
        "results":     outputs,
        "stdout_tail": stdout[-2000:],
        "stderr_tail": stderr[-2000:],
    }

    body = json.dumps(result_obj, indent=2).encode("utf-8")
    print(body.decode("utf-8"))

    # ── Upload result ─────────────────────────────────────────
    if result_uri:
        upload_to_result_uri(
            result_uri=result_uri,
            content=body,
            content_type="application/json",
            aws_profile=_env("AWS_PROFILE") or None,
            azure_auth=_env("AZURE_AUTH", "managed_identity"),
            azure_connection_string=_env("AZURE_STORAGE_CONNECTION_STRING") or None,
        )
        print(f"[generic_sif_runner] uploaded → {result_uri}")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
