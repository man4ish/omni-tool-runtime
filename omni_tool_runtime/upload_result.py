# omni_tool_runtime/upload_result.py  (optional helper module)
from __future__ import annotations

from typing import Optional

from .result_uri import parse_result_uri
from .uploaders.azureblob_uploader import AzureBlobUploader
from .uploaders.s3_uploader import S3Uploader


def upload_to_result_uri(
    *,
    result_uri: str,
    data: bytes,
    content_type: str = "application/json",
    aws_profile: Optional[str] = None,
    azure_auth: str = "managed_identity",
    azure_connection_string: Optional[str] = None,
) -> None:
    parsed = parse_result_uri(result_uri)

    if parsed.scheme == "s3":
        S3Uploader(aws_profile=aws_profile).upload_bytes(
            bucket=parsed.account_or_bucket, key=parsed.path, data=data, content_type=content_type
        )
        return

    if parsed.scheme == "azureblob":
        AzureBlobUploader(
            account_name=parsed.account_or_bucket,
            auth=azure_auth,
            connection_string=azure_connection_string,
        ).upload_bytes(
            container=parsed.container or "",
            blob_path=parsed.path,
            data=data,
            content_type=content_type,
        )
        return

    raise RuntimeError(f"Unsupported scheme: {parsed.scheme}")
