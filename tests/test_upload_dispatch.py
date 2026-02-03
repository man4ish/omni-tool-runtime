# tests/test_upload_dispatch.py
from __future__ import annotations

import pytest

import omni_tool_runtime.upload_result as mod


class _S3Spy:
    def __init__(self, aws_profile=None):
        self.aws_profile = aws_profile
        self.calls = []

    def upload_bytes(self, *, bucket: str, key: str, data: bytes, content_type: str) -> None:
        self.calls.append(
            {"bucket": bucket, "key": key, "data": data, "content_type": content_type, "aws_profile": self.aws_profile}
        )


class _AzureSpy:
    def __init__(self, account_name: str, auth: str = "managed_identity", connection_string=None):
        self.account_name = account_name
        self.auth = auth
        self.connection_string = connection_string
        self.calls = []

    def upload_bytes(self, *, container: str, blob_path: str, data: bytes, content_type: str) -> None:
        self.calls.append(
            {
                "account_name": self.account_name,
                "auth": self.auth,
                "connection_string": self.connection_string,
                "container": container,
                "blob_path": blob_path,
                "data": data,
                "content_type": content_type,
            }
        )


def test_upload_dispatch_to_s3(monkeypatch: pytest.MonkeyPatch):
    s3_spy = _S3Spy(aws_profile="prof1")

    # Patch constructor used inside upload_to_result_uri
    monkeypatch.setattr(mod, "S3Uploader", lambda aws_profile=None: _S3Spy(aws_profile=aws_profile))
    # But we need the created instance to inspect calls. So patch with closure:
    created = {}

    def _ctor(aws_profile=None):
        created["inst"] = _S3Spy(aws_profile=aws_profile)
        return created["inst"]

    monkeypatch.setattr(mod, "S3Uploader", _ctor)

    uri = "s3://bkt/prefix/run1/results.json"
    payload = b'{"ok": true}'
    mod.upload_to_result_uri(result_uri=uri, data=payload, content_type="application/json", aws_profile="prof1")

    inst = created["inst"]
    assert len(inst.calls) == 1
    c = inst.calls[0]
    assert c["bucket"] == "bkt"
    assert c["key"] == "prefix/run1/results.json"
    assert c["data"] == payload
    assert c["content_type"] == "application/json"
    assert c["aws_profile"] == "prof1"


def test_upload_dispatch_to_azureblob(monkeypatch: pytest.MonkeyPatch):
    created = {}

    def _ctor(account_name: str, auth: str = "managed_identity", connection_string=None):
        created["inst"] = _AzureSpy(account_name=account_name, auth=auth, connection_string=connection_string)
        return created["inst"]

    monkeypatch.setattr(mod, "AzureBlobUploader", _ctor)

    uri = "azureblob://acct1/contA/tes-runs/r1/tools/echo_test/results.json"
    payload = b'{"ok": true}'

    mod.upload_to_result_uri(
        result_uri=uri,
        data=payload,
        content_type="application/json",
        azure_auth="connection_string",
        azure_connection_string="UseDevelopmentStorage=true",
    )

    inst = created["inst"]
    assert len(inst.calls) == 1
    c = inst.calls[0]
    assert c["account_name"] == "acct1"
    assert c["auth"] == "connection_string"
    assert c["connection_string"] == "UseDevelopmentStorage=true"
    assert c["container"] == "contA"
    assert c["blob_path"] == "tes-runs/r1/tools/echo_test/results.json"
    assert c["data"] == payload
    assert c["content_type"] == "application/json"


def test_upload_unsupported_scheme_raises():
    with pytest.raises(ValueError):
        mod.upload_to_result_uri(result_uri="gs://bucket/key", data=b"x")
