# omni_tool_runtime/upload_result.py
from __future__ import annotations

import os
from typing import Optional
from urllib.parse import urlparse

from omni_tool_runtime.result_uri import parse_result_uri
from omni_tool_runtime.uploaders.azureblob_uploader import AzureBlobUploader
from omni_tool_runtime.uploaders.s3_uploader import S3Uploader


def _normalize_result_uri(result_uri: str) -> str:
    """
    Accept either:
      - full object URI ending in results.json
      - prefix URI (folder-like), append /results.json
    """
    u = (result_uri or "").strip()
    if not u:
        raise RuntimeError("RESULT_URI not set")

    if u.endswith("/"):
        return u.rstrip("/") + "/results.json"
    if not u.lower().endswith(".json"):
        return u + "/results.json"
    return u


def _parse_s3(uri: str) -> tuple[str, str]:
    # s3://bucket/key
    u = urlparse(uri)
    if u.scheme != "s3":
        raise ValueError(f"Not an s3 URI: {uri}")
    bucket = u.netloc
    key = u.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid s3 URI (need bucket/key): {uri}")
    return bucket, key


def _parse_azureblob(uri: str) -> tuple[str, str, str]:
    # azureblob://<account>/<container>/<blob_path>
    u = urlparse(uri)
    if u.scheme != "azureblob":
        raise ValueError(f"Not an azureblob URI: {uri}")
    account = u.netloc
    path = u.path.lstrip("/")
    parts = path.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid azureblob URI (need container/path): {uri}")
    container, blob_path = parts[0], parts[1]
    if not account or not container or not blob_path:
        raise ValueError(f"Invalid azureblob URI (empty account/container/path): {uri}")
    return account, container, blob_path


def upload_to_result_uri(
    *,
    result_uri: str,
    content: Optional[bytes] = None,
    data: Optional[bytes] = None,
    content_type: str = "application/json",
    aws_profile: Optional[str] = None,
) -> None:
    """
    Upload bytes to RESULT_URI.

    Backward compatible:
      - preferred kw: content=...
      - legacy kw:    data=...
      - either one must be provided

    Extra:
      - result_uri may be a prefix; we auto-append /results.json
      - content_type override
      - aws_profile optional (defaults to env AWS_PROFILE if set)
    """
    payload = content if content is not None else data
    if payload is None:
        raise TypeError("upload_to_result_uri requires bytes via content=... or data=...")

    normalized_uri = _normalize_result_uri(result_uri)

    # Keep your existing parse_result_uri() in the loop so any custom validation stays effective
    info = parse_result_uri(normalized_uri)
    scheme = getattr(info, "scheme", None) or urlparse(normalized_uri).scheme

    if scheme == "s3":
        bucket, key = _parse_s3(normalized_uri)
        prof = aws_profile if aws_profile is not None else (os.getenv("AWS_PROFILE") or None)

        # CHANGED: uploader expects data=, not content=
        S3Uploader(aws_profile=prof).upload_bytes(
            bucket=bucket,
            key=key,
            data=payload,
            content_type=content_type,
        )
        return

    if scheme == "azureblob":
        account, container, blob_path = _parse_azureblob(normalized_uri)

        # CHANGED: uploader expects data=, not content=
        AzureBlobUploader().upload_bytes(
            account=account,
            container=container,
            blob_path=blob_path,
            data=payload,
            content_type=content_type,
        )
        return

    raise ValueError(f"Unsupported RESULT_URI scheme: {scheme}")
