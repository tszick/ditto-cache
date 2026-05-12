#!/usr/bin/env python3
"""Fail when generated or local build artifacts are tracked by git."""

from __future__ import annotations

import subprocess
import sys


FORBIDDEN_PREFIXES = (
    "target/",
    "build-out/",
    "ditto-nodejs-client/node_modules/",
    "ditto-nodejs-client/dist/",
    "ditto-java-client/build/",
    "ditto-java-client/.gradle/",
)

FORBIDDEN_PARTS = (
    "/__pycache__/",
)

FORBIDDEN_SUFFIXES = (
    ".pyc",
    ".pyo",
    ".rs.bk",
)


def git_ls_files() -> list[str]:
    output = subprocess.check_output(["git", "ls-files"], text=True)
    return [line.strip().replace("\\", "/") for line in output.splitlines() if line.strip()]


def is_forbidden(path: str) -> bool:
    return (
        path.startswith(FORBIDDEN_PREFIXES)
        or any(part in path for part in FORBIDDEN_PARTS)
        or path.endswith(FORBIDDEN_SUFFIXES)
    )


def main() -> int:
    bad = [path for path in git_ls_files() if is_forbidden(path)]
    if bad:
        print("repo hygiene validation failed: generated artifacts are tracked:", file=sys.stderr)
        for path in bad:
            print(f"  - {path}", file=sys.stderr)
        print("remove them from git tracking and keep them ignored", file=sys.stderr)
        return 1
    print("repo hygiene OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
