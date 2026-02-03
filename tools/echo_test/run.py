# tools/echo_test/run.py
from __future__ import annotations

import json
import os
import sys

from omni_tool_runtime.contract import read_contract_from_env
from omni_tool_runtime.upload_result import upload_to_result_uri


def main() -> int:
    c = read_contract_from_env()

    # your tool contract: expects inputs.text
    text = c.inputs.get("text")

    if text is None:
        result_obj = {
            "ok": False,
            "error": "missing inputs.text",
            "tool_id": c.tool_id,
            "run_id": c.run_id,
            "inputs": c.inputs,
        }
        exit_code = 2
    else:
        result_obj = {
            "ok": True,
            "tool_id": c.tool_id,
            "run_id": c.run_id,
            "results": {"echo": text},
        }
        exit_code = 0

    body = json.dumps(result_obj, indent=2).encode("utf-8")

    # Always print to logs (helpful for debugging)
    print(body.decode("utf-8"))

    if not c.result_uri:
        # No RESULT_URI means "log-only" mode; still return nonzero to surface misconfig in TES
        print("ERROR: RESULT_URI not set", file=sys.stderr)
        return 3

    # Adapter passes RESULT_URI as a prefix; upload results.json under it.
    uri = c.result_uri.rstrip("/") + "/results.json"

    # Optional env overrides for local dev
    azure_auth = os.getenv("AZURE_AUTH", "managed_identity")
    azure_cs = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    aws_profile = os.getenv("AWS_PROFILE")

    upload_to_result_uri(
        result_uri=uri,
        data=body,
        content_type="application/json",
        aws_profile=aws_profile,
        azure_auth=azure_auth,
        azure_connection_string=azure_cs,
    )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
