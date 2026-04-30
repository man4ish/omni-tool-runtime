# tests/test_result_uri_parse.py
from __future__ import annotations

import pytest

from omni_tool_runtime.result_uri import parse_result_uri


def test_parse_s3_uri_ok():
    p = parse_result_uri("s3://my-bucket/some/prefix/results.json")
    assert p.scheme == "s3"
    assert p.account_or_bucket == "my-bucket"
    assert p.container is None
    assert p.path == "some/prefix/results.json"


def test_parse_s3_uri_requires_bucket_and_key():
    with pytest.raises(ValueError):
        parse_result_uri("s3://")
    with pytest.raises(ValueError):
        parse_result_uri("s3://bucket-only")
    with pytest.raises(ValueError):
        parse_result_uri("s3://bucket/")


def test_parse_azureblob_uri_ok():
    p = parse_result_uri("azureblob://acct/container/path/to/results.json")
    assert p.scheme == "azureblob"
    assert p.account_or_bucket == "acct"
    assert p.container == "container"
    assert p.path == "path/to/results.json"


def test_parse_azureblob_uri_requires_container_and_path():
    with pytest.raises(ValueError):
        parse_result_uri("azureblob://acct/")
    with pytest.raises(ValueError):
        parse_result_uri("azureblob://acct/container")
    with pytest.raises(ValueError):
        parse_result_uri("azureblob://acct/container/")


def test_parse_gs_uri_ok():
    p = parse_result_uri("gs://my-bucket/some/prefix/results.json")
    assert p.scheme == "gs"
    assert p.account_or_bucket == "my-bucket"
    assert p.container is None
    assert p.path == "some/prefix/results.json"


def test_parse_gs_uri_requires_bucket_and_key():
    with pytest.raises(ValueError):
        parse_result_uri("gs://")
    with pytest.raises(ValueError):
        parse_result_uri("gs://bucket-only")
    with pytest.raises(ValueError):
        parse_result_uri("gs://bucket/")


def test_parse_rejects_unknown_scheme():
    with pytest.raises(ValueError, match="Unsupported"):
        parse_result_uri("ftp://bucket/key")  # ftp is not supported


def test_parse_rejects_missing_scheme():
    with pytest.raises(ValueError, match="missing scheme"):
        parse_result_uri("bucket/key")
