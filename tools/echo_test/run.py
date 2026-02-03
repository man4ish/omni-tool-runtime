# tools/echo_test/run.py
from __future__ import annotations

import json
import os
import sys

from omni_tool_runtime.upload_result import upload_to_result_uri


def main() -> int:
    tool_id = os.getenv("TOOL_ID", "")
    run_id = os.getenv("RUN_ID", "")
    result_uri = (os.getenv("RESULT_URI", "") or "").strip()

    inputs_json = os.getenv("INPUTS_JSON", "{}")
    try:
        inputs = json.loads(inputs_json)
    except Exception as e:
        out = {"ok": False, "error": f"bad INPUTS_JSON: {e}", "tool_id": tool_id, "run_id": run_id}
        print(json.dumps(out))
        return 2

    text = inputs.get("text")  # keep strict contract (or: inputs.get("text") or inputs.get("msg"))
    if text is None:
        result_obj = {
            "ok": False,
            "error": "missing inputs.text",
            "tool_id": tool_id,
            "run_id": run_id,
            "inputs": inputs,
        }
    else:
        result_obj = {
            "ok": True,
            "tool_id": tool_id,
            "run_id": run_id,
            "results": {"echo": text},
        }

    body = json.dumps(result_obj, indent=2)

    # Always print for logs/debug
    print(body)

    # If RESULT_URI not set -> local mode: succeed
    if not result_uri:
        return 0

    # Cloud mode: upload
    upload_to_result_uri(result_uri=result_uri, content=body.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
