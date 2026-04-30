"""
Unit tests for tools/generic_sif_runner/run.py

Covers every function and every branch in main():
  - _env
  - _resolve_env_refs
  - _fetch_sif  (local / s3 / azure / cache-hit)
  - _fetch_from_s3  (awscli success / fallback boto3 / both fail)
  - _fetch_from_azure  (connection_string / managed_identity / failure)
  - _load_tool_def  (TOOL_DEF_JSON / TOOL_DEF_PATH / TES_URL / all-missing)
  - _resolve_command  (happy path / missing key)
  - _collect_outputs  (1 match / many matches / no match)
  - main()  (every early-exit path + success + upload)
  - __main__ block
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, mock_open, patch

import pytest

import tools.generic_sif_runner.run as sif_run
from tools.generic_sif_runner.run import (
    _collect_outputs,
    _env,
    _fetch_from_azure,
    _fetch_from_s3,
    _fetch_sif,
    _load_tool_def,
    _resolve_command,
    _resolve_env_refs,
    main,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

MINIMAL_TOOL_DEF = {
    "slurm": {
        "image": "/sif/tool.sif",
        "command": ["echo", "hi"],
        "outputs": [],
    }
}

def _base_env(
    tool_id: str = "tool-1",
    run_id: str = "run-1",
    result_uri: str = "",
    inputs_json: str = "{}",
    resources_json: str = "{}",
    tool_def_json: str = "",
    work_dir: str = "",
    sif_cache_dir: str = "/tmp/test_sif_cache",
) -> dict[str, str]:
    env: dict[str, str] = {
        "TOOL_ID": tool_id,
        "RUN_ID": run_id,
        "RESULT_URI": result_uri,
        "INPUTS_JSON": inputs_json,
        "RESOURCES_JSON": resources_json,
        "SIF_CACHE_DIR": sif_cache_dir,
    }
    if tool_def_json:
        env["TOOL_DEF_JSON"] = tool_def_json
    if work_dir:
        env["WORK_DIR"] = work_dir
    return env


def _env_with_tool_def(tool_def: dict = None, **kwargs) -> dict[str, str]:
    td = tool_def or MINIMAL_TOOL_DEF
    return _base_env(tool_def_json=json.dumps(td), **kwargs)


# ===========================================================================
# 1. _env()
# ===========================================================================
class TestEnvHelper:

    def test_returns_env_value(self):
        with patch.dict("os.environ", {"MY_VAR": "hello"}, clear=False):
            assert _env("MY_VAR") == "hello"

    def test_returns_default_when_missing(self):
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("MISSING_VAR", None)
            assert _env("MISSING_VAR", "fallback") == "fallback"

    def test_empty_string_env_uses_default(self):
        """An empty string env var is falsy → should return default."""
        with patch.dict("os.environ", {"MY_VAR": ""}, clear=False):
            assert _env("MY_VAR", "default") == "default"

    def test_default_is_empty_string_when_not_given(self):
        os.environ.pop("TOTALLY_ABSENT", None)
        assert _env("TOTALLY_ABSENT") == ""

    def test_returns_string_type(self):
        with patch.dict("os.environ", {"NUM_VAR": "42"}, clear=False):
            result = _env("NUM_VAR")
        assert isinstance(result, str)


# ===========================================================================
# 2. _resolve_env_refs()
# ===========================================================================
class TestResolveEnvRefs:

    def test_dollar_brace_syntax(self):
        with patch.dict("os.environ", {"MY_VAR": "world"}, clear=False):
            assert _resolve_env_refs("hello ${MY_VAR}") == "hello world"

    def test_dollar_plain_syntax(self):
        with patch.dict("os.environ", {"MY_VAR": "world"}, clear=False):
            assert _resolve_env_refs("hello $MY_VAR") == "hello world"

    def test_missing_var_kept_as_is(self):
        os.environ.pop("ABSENT_VAR", None)
        result = _resolve_env_refs("${ABSENT_VAR}")
        assert result == "${ABSENT_VAR}"

    def test_multiple_vars_expanded(self):
        with patch.dict("os.environ", {"A": "foo", "B": "bar"}, clear=False):
            assert _resolve_env_refs("${A}-${B}") == "foo-bar"

    def test_no_vars_unchanged(self):
        assert _resolve_env_refs("no vars here") == "no vars here"

    def test_empty_string(self):
        assert _resolve_env_refs("") == ""

    def test_mixed_syntax(self):
        with patch.dict("os.environ", {"X": "1", "Y": "2"}, clear=False):
            assert _resolve_env_refs("${X} $Y") == "1 2"


# ===========================================================================
# 3. _fetch_sif()
# ===========================================================================
class TestFetchSif:

    # --- local path ---
    def test_local_path_returns_path_object(self, tmp_path):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        result = _fetch_sif(str(sif), tmp_path)
        assert result == sif

    def test_local_path_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="SIF not found"):
            _fetch_sif("/nonexistent/path/tool.sif", tmp_path)

    # --- cache hit ---
    def test_s3_cache_hit_returns_cached(self, tmp_path):
        cached = tmp_path / "tool.sif"
        cached.write_bytes(b"x" * 1024 * 1024 * 5)  # 5 MB
        result = _fetch_sif("s3://bucket/tool.sif", tmp_path)
        assert result == cached

    def test_azure_cache_hit_returns_cached(self, tmp_path):
        cached = tmp_path / "tool.sif"
        cached.write_bytes(b"data")
        result = _fetch_sif("azureblob://account/container/tool.sif", tmp_path)
        assert result == cached

    def test_cache_hit_prints_message(self, tmp_path, capsys):
        cached = tmp_path / "tool.sif"
        cached.write_bytes(b"x" * 1024 * 1024)
        _fetch_sif("s3://bucket/tool.sif", tmp_path)
        assert "hit" in capsys.readouterr().out

    # --- s3 download ---
    def test_s3_miss_calls_fetch_from_s3(self, tmp_path):
        cache_dir = tmp_path / "cache"
        with patch("tools.generic_sif_runner.run._fetch_from_s3") as mock_s3:
            mock_s3.side_effect = lambda uri, dest: dest.write_bytes(b"sif")
            result = _fetch_sif("s3://bucket/tool.sif", cache_dir)
        mock_s3.assert_called_once()

    def test_s3_miss_creates_cache_dir(self, tmp_path):
        cache_dir = tmp_path / "new_cache"
        with patch("tools.generic_sif_runner.run._fetch_from_s3") as mock_s3:
            mock_s3.side_effect = lambda uri, dest: dest.write_bytes(b"sif")
            _fetch_sif("s3://bucket/tool.sif", cache_dir)
        assert cache_dir.exists()

    # --- azure download ---
    def test_azure_miss_calls_fetch_from_azure(self, tmp_path):
        cache_dir = tmp_path / "cache"
        with patch("tools.generic_sif_runner.run._fetch_from_azure") as mock_az:
            mock_az.side_effect = lambda uri, dest: dest.write_bytes(b"sif")
            _fetch_sif("azureblob://account/container/tool.sif", cache_dir)
        mock_az.assert_called_once()

    # --- env var expansion ---
    def test_env_var_in_uri_expanded(self, tmp_path):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        with patch.dict("os.environ", {"SIF_PATH": str(sif)}, clear=False):
            result = _fetch_sif("${SIF_PATH}", tmp_path / "cache")
        assert result == sif


# ===========================================================================
# 4. _fetch_from_s3()
# ===========================================================================
class TestFetchFromS3:

    def _make_boto3_mock(self):
        """Return a boto3 mock whose download_file writes the dest file."""
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value.download_file.side_effect = (
            lambda bucket, key, dst: Path(dst).write_bytes(b"sif-data")
        )
        return mock_boto3

    def test_boto3_download_success(self, tmp_path):
        dest = tmp_path / "tool.sif"
        with patch.dict("sys.modules", {"boto3": self._make_boto3_mock()}):
            _fetch_from_s3("s3://bucket/tool.sif", dest)
        assert dest.exists()

    def test_boto3_called_with_correct_bucket_and_key(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_boto3 = self._make_boto3_mock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            _fetch_from_s3("s3://my-bucket/my/key.sif", dest)
        call_args = mock_boto3.client.return_value.download_file.call_args[0]
        assert call_args[0] == "my-bucket"
        assert call_args[1] == "my/key.sif"

    def test_boto3_called_once_per_download(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_boto3 = self._make_boto3_mock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            _fetch_from_s3("s3://bucket/tool.sif", dest)
        mock_boto3.client.return_value.download_file.assert_called_once()

    def test_boto3_creates_parent_directory(self, tmp_path):
        dest = tmp_path / "subdir" / "tool.sif"
        with patch.dict("sys.modules", {"boto3": self._make_boto3_mock()}):
            _fetch_from_s3("s3://bucket/tool.sif", dest)
        assert dest.parent.exists()

    def test_both_fail_raises_runtime_error(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value.download_file.side_effect = Exception("boom")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with patch.dict("sys.modules", {"boto3": mock_boto3}):
                with pytest.raises(RuntimeError, match="S3 download failed"):
                    _fetch_from_s3("s3://bucket/tool.sif", dest)

    def test_runtime_error_message_contains_uri(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value.download_file.side_effect = Exception("x")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with patch.dict("sys.modules", {"boto3": mock_boto3}):
                with pytest.raises(RuntimeError) as exc_info:
                    _fetch_from_s3("s3://bucket/tool.sif", dest)
        assert "s3://bucket/tool.sif" in str(exc_info.value)


# ===========================================================================
# 5. _fetch_from_azure()
# ===========================================================================
class TestFetchFromAzure:

    def _make_azure_mocks(self):
        mock_blob_data = MagicMock()
        mock_blob_data.readall.return_value = b"sif-bytes"
        mock_bc = MagicMock()
        mock_bc.download_blob.return_value = mock_blob_data
        mock_svc = MagicMock()
        mock_svc.get_blob_client.return_value = mock_bc
        mock_bsc_cls = MagicMock(return_value=mock_svc)
        mock_bsc_cls.from_connection_string = MagicMock(return_value=mock_svc)
        return mock_bsc_cls, mock_svc, mock_bc

    def test_managed_identity_path(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_bsc_cls, mock_svc, _ = self._make_azure_mocks()
        mock_cred = MagicMock()
        env = {"AZURE_AUTH": "managed_identity", "AZURE_STORAGE_CONNECTION_STRING": ""}
        with patch.dict("os.environ", env, clear=False):
            with patch.dict("sys.modules", {
                "azure.storage.blob": MagicMock(BlobServiceClient=mock_bsc_cls),
                "azure.identity": MagicMock(DefaultAzureCredential=MagicMock(return_value=mock_cred)),
            }):
                _fetch_from_azure("azureblob://account/container/tool.sif", dest)
        assert dest.read_bytes() == b"sif-bytes"

    def test_connection_string_path(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_bsc_cls, mock_svc, _ = self._make_azure_mocks()
        env = {"AZURE_AUTH": "connection_string", "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https"}
        with patch.dict("os.environ", env, clear=False):
            with patch.dict("sys.modules", {
                "azure.storage.blob": MagicMock(BlobServiceClient=mock_bsc_cls),
                "azure.identity": MagicMock(),
            }):
                _fetch_from_azure("azureblob://account/container/tool.sif", dest)
        mock_bsc_cls.from_connection_string.assert_called_once()

    def test_failure_raises_runtime_error(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_bsc_cls = MagicMock(side_effect=Exception("azure boom"))
        with patch.dict("os.environ", {"AZURE_AUTH": "managed_identity"}, clear=False):
            with patch.dict("sys.modules", {
                "azure.storage.blob": MagicMock(BlobServiceClient=mock_bsc_cls),
                "azure.identity": MagicMock(),
            }):
                with pytest.raises(RuntimeError, match="Azure Blob download failed"):
                    _fetch_from_azure("azureblob://account/container/tool.sif", dest)

    def test_container_and_blob_parsed_correctly(self, tmp_path):
        dest = tmp_path / "tool.sif"
        mock_bsc_cls, mock_svc, mock_bc = self._make_azure_mocks()
        env = {"AZURE_AUTH": "managed_identity", "AZURE_STORAGE_CONNECTION_STRING": ""}
        with patch.dict("os.environ", env, clear=False):
            with patch.dict("sys.modules", {
                "azure.storage.blob": MagicMock(BlobServiceClient=mock_bsc_cls),
                "azure.identity": MagicMock(DefaultAzureCredential=MagicMock()),
            }):
                _fetch_from_azure("azureblob://account/mycontainer/deep/path.sif", dest)
        call_kwargs = mock_svc.get_blob_client.call_args[1]
        assert call_kwargs["container"] == "mycontainer"
        assert call_kwargs["blob"] == "deep/path.sif"


# ===========================================================================
# 6. _load_tool_def()
# ===========================================================================
class TestLoadToolDef:

    def test_loads_from_tool_def_json_env(self):
        td = {"slurm": {"image": "/sif/tool.sif"}}
        with patch.dict("os.environ", {"TOOL_DEF_JSON": json.dumps(td)}, clear=False):
            result = _load_tool_def()
        assert result == td

    def test_tool_def_json_takes_priority_over_path(self, tmp_path):
        td_json = {"source": "env"}
        td_file = {"source": "file"}
        p = tmp_path / "tool.json"
        p.write_text(json.dumps(td_file))
        env = {"TOOL_DEF_JSON": json.dumps(td_json), "TOOL_DEF_PATH": str(p)}
        with patch.dict("os.environ", env, clear=False):
            result = _load_tool_def()
        assert result["source"] == "env"

    def test_loads_from_tool_def_path_env(self, tmp_path):
        td = {"slurm": {"image": "/sif/tool.sif"}}
        p = tmp_path / "tool.json"
        p.write_text(json.dumps(td))
        env = {"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": str(p)}
        with patch.dict("os.environ", env, clear=False):
            result = _load_tool_def()
        assert result == td

    def test_tool_def_path_missing_file_skipped(self, tmp_path):
        """A TOOL_DEF_PATH pointing to a nonexistent file falls through to TES."""
        env = {
            "TOOL_DEF_JSON": "",
            "TOOL_DEF_PATH": str(tmp_path / "no_such.json"),
            "TES_URL": "",
            "TOOL_ID": "",
        }
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(RuntimeError, match="Cannot load tool definition"):
                _load_tool_def()

    def test_loads_from_tes_url(self):
        tool_id = "my-tool"
        td = {"tool_id": tool_id, "slurm": {}}
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([td]).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        env = {"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": "", "TES_URL": "http://tes:8081", "TOOL_ID": tool_id}
        with patch.dict("os.environ", env, clear=False):
            with patch("urllib.request.urlopen", return_value=mock_response):
                result = _load_tool_def()
        assert result["tool_id"] == tool_id

    def test_all_missing_raises_runtime_error(self):
        env = {"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": "", "TES_URL": "", "TOOL_ID": ""}
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(RuntimeError, match="Cannot load tool definition"):
                _load_tool_def()

    def test_error_message_mentions_env_vars(self):
        env = {"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": "", "TES_URL": "", "TOOL_ID": ""}
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(RuntimeError) as exc_info:
                _load_tool_def()
        msg = str(exc_info.value)
        assert "TOOL_DEF_JSON" in msg or "TES_URL" in msg


# ===========================================================================
# 7. _resolve_command()
# ===========================================================================
class TestResolveCommand:

    def test_simple_substitution(self):
        result = _resolve_command(["echo", "{msg}"], {"msg": "hello"}, "/work")
        assert result == ["echo", "hello"]

    def test_work_dir_substituted(self):
        result = _resolve_command(["{work_dir}/out.bam"], {}, "/work/dir")
        assert result == ["/work/dir/out.bam"]

    def test_multiple_inputs_substituted(self):
        result = _resolve_command(
            ["tool", "--in", "{infile}", "--out", "{outfile}"],
            {"infile": "/a.bam", "outfile": "/b.bam"},
            "/work",
        )
        assert result == ["tool", "--in", "/a.bam", "--out", "/b.bam"]

    def test_missing_key_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="Missing input for command placeholder"):
            _resolve_command(["{missing_key}"], {}, "/work")

    def test_missing_key_error_mentions_key_name(self):
        with pytest.raises(RuntimeError) as exc_info:
            _resolve_command(["{my_missing_key}"], {}, "/work")
        assert "my_missing_key" in str(exc_info.value)

    def test_no_placeholders_returned_as_is(self):
        cmd = ["singularity", "exec", "tool.sif", "echo"]
        assert _resolve_command(cmd, {}, "/work") == cmd

    def test_returns_list_of_strings(self):
        result = _resolve_command(["echo", "{val}"], {"val": "x"}, "/w")
        assert all(isinstance(r, str) for r in result)


# ===========================================================================
# 8. _collect_outputs()
# ===========================================================================
class TestCollectOutputs:

    def test_single_match_stored_as_string(self, tmp_path):
        (tmp_path / "output.bam").write_bytes(b"")
        result = _collect_outputs(tmp_path, [{"name": "bam", "pattern": "*.bam"}])
        assert result["bam"] == str(tmp_path / "output.bam")

    def test_multiple_matches_stored_as_list(self, tmp_path):
        (tmp_path / "a.bam").write_bytes(b"")
        (tmp_path / "b.bam").write_bytes(b"")
        result = _collect_outputs(tmp_path, [{"name": "bam", "pattern": "*.bam"}])
        assert isinstance(result["bam"], list)
        assert len(result["bam"]) == 2

    def test_no_match_stored_as_none(self, tmp_path):
        result = _collect_outputs(tmp_path, [{"name": "vcf", "pattern": "*.vcf"}])
        assert result["vcf"] is None

    def test_no_match_prints_warning(self, tmp_path, capsys):
        _collect_outputs(tmp_path, [{"name": "vcf", "pattern": "*.vcf"}])
        assert "WARNING" in capsys.readouterr().out

    def test_default_name_is_output(self, tmp_path):
        (tmp_path / "file.txt").write_bytes(b"")
        result = _collect_outputs(tmp_path, [{"pattern": "*.txt"}])
        assert "output" in result

    def test_default_pattern_matches_all(self, tmp_path):
        (tmp_path / "anything.xyz").write_bytes(b"")
        result = _collect_outputs(tmp_path, [{"name": "out"}])
        assert result["out"] is not None

    def test_multiple_specs_returned(self, tmp_path):
        (tmp_path / "a.bam").write_bytes(b"")
        (tmp_path / "b.vcf").write_bytes(b"")
        result = _collect_outputs(tmp_path, [
            {"name": "bam", "pattern": "*.bam"},
            {"name": "vcf", "pattern": "*.vcf"},
        ])
        assert "bam" in result and "vcf" in result

    def test_empty_spec_list_returns_empty_dict(self, tmp_path):
        assert _collect_outputs(tmp_path, []) == {}

    def test_matches_are_sorted(self, tmp_path):
        (tmp_path / "z.bam").write_bytes(b"")
        (tmp_path / "a.bam").write_bytes(b"")
        result = _collect_outputs(tmp_path, [{"name": "bam", "pattern": "*.bam"}])
        assert result["bam"] == sorted(result["bam"])


# ===========================================================================
# 9. main() — early-exit paths
# ===========================================================================
class TestMainEarlyExits:

    def test_bad_inputs_json_returns_2(self):
        env = _base_env(inputs_json="not-json")
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_bad_resources_json_returns_2(self):
        env = _base_env(resources_json="{bad}")
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_bad_json_prints_to_stderr(self, capsys):
        env = _base_env(inputs_json="bad")
        with patch.dict("os.environ", env, clear=True):
            main()
        assert "ERROR" in capsys.readouterr().err

    def test_load_tool_def_failure_returns_2(self):
        env = _base_env()   # no TOOL_DEF_JSON → will fail
        env.update({"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": "", "TES_URL": "", "TOOL_ID": ""})
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_load_tool_def_failure_prints_error(self, capsys):
        env = _base_env()
        env.update({"TOOL_DEF_JSON": "", "TOOL_DEF_PATH": "", "TES_URL": "", "TOOL_ID": ""})
        with patch.dict("os.environ", env, clear=True):
            main()
        assert "ERROR" in capsys.readouterr().err

    def test_missing_slurm_image_returns_2(self):
        td = {"slurm": {"image": "", "command": [], "outputs": []}}
        env = _env_with_tool_def(td)
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_missing_slurm_image_prints_error(self, capsys):
        td = {"slurm": {"image": "", "command": [], "outputs": []}}
        env = _env_with_tool_def(td)
        with patch.dict("os.environ", env, clear=True):
            main()
        assert "no slurm.image" in capsys.readouterr().err

    def test_no_slurm_key_returns_2(self):
        td = {}
        env = _env_with_tool_def(td)
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_sif_fetch_failure_returns_2(self, tmp_path):
        td = {"slurm": {"image": "/nonexistent/tool.sif", "command": ["echo"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_sif_fetch_failure_prints_error(self, tmp_path, capsys):
        td = {"slurm": {"image": "/nonexistent/tool.sif", "command": ["echo"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        with patch.dict("os.environ", env, clear=True):
            main()
        assert "SIF fetch failed" in capsys.readouterr().err

    def test_resolve_command_failure_returns_2(self, tmp_path):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["{missing}"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        with patch.dict("os.environ", env, clear=True):
            assert main() == 2

    def test_resolve_command_failure_prints_error(self, tmp_path, capsys):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["{missing}"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        with patch.dict("os.environ", env, clear=True):
            main()
        assert "ERROR" in capsys.readouterr().err


# ===========================================================================
# 10. main() — successful execution
# ===========================================================================
class TestMainSuccess:

    def _run_with_mock_proc(
        self,
        tmp_path,
        returncode: int = 0,
        stdout: str = "done",
        stderr: str = "",
        result_uri: str = "",
        inputs_json: str = "{}",
        resources_json: str = "{}",
    ):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["echo", "hi"], "outputs": []}}
        env = _env_with_tool_def(
            td,
            work_dir=str(tmp_path),
            result_uri=result_uri,
            inputs_json=inputs_json,
            resources_json=resources_json,
        )
        mock_proc = MagicMock()
        mock_proc.returncode = returncode
        mock_proc.stdout = stdout
        mock_proc.stderr = stderr
        return env, mock_proc

    def test_returns_0_on_success(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                rc = main()
        assert rc == 0

    def test_returns_1_when_singularity_fails(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path, returncode=1)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                rc = main()
        assert rc == 1

    def test_exit_code_in_result(self, tmp_path, capsys):
        env, mock_proc = self._run_with_mock_proc(tmp_path, returncode=0)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                main()
        out = capsys.readouterr().out
        # Find result JSON
        for chunk in reversed(out.strip().split("\n\n")):
            try:
                obj = json.loads(chunk.strip())
                if "exit_code" in obj:
                    assert obj["exit_code"] == 0
                    return
            except Exception:
                pass

    def test_tool_id_in_result(self, tmp_path, capsys):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        env["TOOL_ID"] = "my-tool"
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                main()
        out = capsys.readouterr().out
        assert "my-tool" in out

    def test_singularity_called_with_exec(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        args = mock_run.call_args[0][0]
        assert args[0] == "singularity"
        assert args[1] == "exec"

    def test_singularity_cmd_includes_sif_path(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        sif_path = str(tmp_path / "tool.sif")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        args = mock_run.call_args[0][0]
        assert sif_path in args

    def test_omp_num_threads_set_from_resources(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path, resources_json='{"cpu": 4}')
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        passed_env = mock_run.call_args[1]["env"]
        assert passed_env["OMP_NUM_THREADS"] == "4"

    def test_omp_num_threads_defaults_to_1(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        passed_env = mock_run.call_args[1]["env"]
        assert passed_env["OMP_NUM_THREADS"] == "1"

    def test_stderr_printed_to_stderr_stream(self, tmp_path, capsys):
        env, mock_proc = self._run_with_mock_proc(tmp_path, stderr="some error")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                main()
        assert "some error" in capsys.readouterr().err

    def test_work_dir_bound_in_singularity_cmd(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(tmp_path)
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        args = mock_run.call_args[0][0]
        assert "--bind" in args

    def test_input_file_path_bound_if_exists(self, tmp_path):
        input_file = tmp_path / "input.bam"
        input_file.write_bytes(b"data")
        env, mock_proc = self._run_with_mock_proc(
            tmp_path,
            inputs_json=json.dumps({"infile": str(input_file)}),
        )
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        args = mock_run.call_args[0][0]
        # The parent directory should be bound
        assert str(tmp_path) in " ".join(args)

    def test_non_path_input_not_bound(self, tmp_path):
        env, mock_proc = self._run_with_mock_proc(
            tmp_path,
            inputs_json=json.dumps({"count": "42"}),
        )
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                main()
        args = mock_run.call_args[0][0]
        # "42" should not appear as a bind path
        bind_vals = [args[i + 1] for i, a in enumerate(args) if a == "--bind"]
        assert not any("42" in b for b in bind_vals)


# ===========================================================================
# 11. main() — upload path
# ===========================================================================
class TestMainUpload:

    def _run_with_upload(self, tmp_path, result_uri: str, upload_mock):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["echo", "hi"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path), result_uri=result_uri)
        mock_proc = MagicMock(returncode=0, stdout="ok", stderr="")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                with patch("tools.generic_sif_runner.run.upload_to_result_uri", upload_mock):
                    return main()

    def test_upload_called_when_result_uri_set(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key", mock_upload)
        mock_upload.assert_called_once()

    def test_upload_not_called_without_result_uri(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "", mock_upload)
        mock_upload.assert_not_called()

    def test_upload_receives_result_uri(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key.json", mock_upload)
        _, kwargs = mock_upload.call_args
        assert kwargs["result_uri"] == "s3://bucket/key.json"

    def test_upload_content_is_bytes(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key", mock_upload)
        _, kwargs = mock_upload.call_args
        assert isinstance(kwargs["content"], bytes)

    def test_upload_content_is_valid_json(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key", mock_upload)
        _, kwargs = mock_upload.call_args
        obj = json.loads(kwargs["content"].decode("utf-8"))
        assert "ok" in obj

    def test_upload_content_type_is_json(self, tmp_path):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key", mock_upload)
        _, kwargs = mock_upload.call_args
        assert kwargs["content_type"] == "application/json"

    def test_upload_uses_aws_profile_from_env(self, tmp_path):
        mock_upload = MagicMock()
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["echo"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path), result_uri="s3://b/k")
        env["AWS_PROFILE"] = "my-profile"
        mock_proc = MagicMock(returncode=0, stdout="", stderr="")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                with patch("tools.generic_sif_runner.run.upload_to_result_uri", mock_upload):
                    main()
        _, kwargs = mock_upload.call_args
        assert kwargs["aws_profile"] == "my-profile"

    def test_upload_prints_confirmation(self, tmp_path, capsys):
        mock_upload = MagicMock()
        self._run_with_upload(tmp_path, "s3://bucket/key", mock_upload)
        assert "uploaded" in capsys.readouterr().out


# ===========================================================================
# 12. __main__ block
# ===========================================================================
class TestMainBlock:

    def test_raises_system_exit_on_success(self, tmp_path):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["echo"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        mock_proc = MagicMock(returncode=0, stdout="", stderr="")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                with pytest.raises(SystemExit) as exc_info:
                    raise SystemExit(main())
        assert exc_info.value.code == 0

    def test_raises_system_exit_with_code_2_on_bad_json(self):
        env = _base_env(inputs_json="bad")
        with patch.dict("os.environ", env, clear=True):
            with pytest.raises(SystemExit) as exc_info:
                raise SystemExit(main())
        assert exc_info.value.code == 2

    def test_raises_system_exit_with_code_1_on_tool_failure(self, tmp_path):
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"fake")
        td = {"slurm": {"image": str(sif), "command": ["false"], "outputs": []}}
        env = _env_with_tool_def(td, work_dir=str(tmp_path))
        mock_proc = MagicMock(returncode=1, stdout="", stderr="failed")
        with patch.dict("os.environ", env, clear=True):
            with patch("subprocess.run", return_value=mock_proc):
                with pytest.raises(SystemExit) as exc_info:
                    raise SystemExit(main())
        assert exc_info.value.code == 1