#!/usr/bin/env python3
"""
Flatten omnibioai-tool-runtime source code into a single file
for human / LLM review.

Includes:
- All .py files
- Dockerfile

Excludes:
- __pycache__ directories
- .venv / .git / node_modules
- binary or compiled files
"""

from pathlib import Path

ROOT = Path(".").resolve()
OUTPUT = Path("omnibioai-tool-runtime.ALL_CODE.txt")

EXCLUDE_DIRS = {
    "__pycache__",
    ".git",
    ".venv",
    "node_modules",
    "dist",
    "build",
}


def should_skip(path: Path) -> bool:
    return any(part in EXCLUDE_DIRS for part in path.parts)


def main():
    files = []

    # Dockerfile (first)
    dockerfile = ROOT / "Dockerfile"
    if dockerfile.exists():
        files.append(dockerfile)

    # All .py files
    for path in sorted(ROOT.rglob("*.py")):
        if should_skip(path):
            continue
        files.append(path)

    with OUTPUT.open("w", encoding="utf-8") as out:
        out.write("# ==================================================\n")
        out.write("# OMNIBIOAI TOOL RUNTIME — FLATTENED SOURCE FOR REVIEW\n")
        out.write("# ==================================================\n\n")

        for path in files:
            rel = path.relative_to(ROOT)
            out.write("\n\n")
            out.write("# --------------------------------------------------\n")
            out.write(f"# FILE: {rel}\n")
            out.write("# --------------------------------------------------\n\n")

            try:
                out.write(path.read_text(encoding="utf-8"))
            except Exception as e:
                out.write(f"# ERROR READING FILE: {e}\n")

    print(f"✅ Wrote {OUTPUT} ({OUTPUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
