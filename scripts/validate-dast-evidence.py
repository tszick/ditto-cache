#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path


PASS_STATUSES = {"pass", "warn-only-pass"}
SEVERITY_FIELDS = {
    "critical": "critical_count",
    "high": "high_count",
    "medium": "medium_count",
    "low": "low_count",
}


def parse_date(value):
    if not isinstance(value, str) or not value:
        raise ValueError("date value must be a non-empty string")
    normalized = value.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(normalized)
    if isinstance(parsed, dt.datetime):
        return parsed.date()
    return parsed


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fail(errors, message):
    errors.append(message)


def validate_report_files(repo_root, tool, errors, require_report_files):
    for index, report_file in enumerate(tool.get("report_files", [])):
        path_value = report_file.get("path")
        expected_hash = str(report_file.get("sha256", "")).lower()
        if not path_value:
            fail(errors, f"{tool['name']}: report_files[{index}].path is required")
            continue
        if not expected_hash:
            fail(errors, f"{tool['name']}: report_files[{index}].sha256 is required")
            continue

        path = repo_root / path_value
        if not path.exists():
            if require_report_files or not report_file.get("local_only", False):
                fail(errors, f"{tool['name']}: report file is missing: {path_value}")
            continue

        actual_hash = sha256_file(path)
        if actual_hash != expected_hash:
            fail(
                errors,
                f"{tool['name']}: sha256 mismatch for {path_value}: expected {expected_hash}, got {actual_hash}",
            )


def validate_tool(repo_root, tool, evidence, today, args, errors):
    name = tool.get("name")
    if not name:
        fail(errors, "tool.name is required")
        return

    status = tool.get("status")
    if tool.get("required_for_release") and status not in PASS_STATUSES:
        fail(errors, f"{name}: required tool status must be one of {sorted(PASS_STATUSES)}, got {status!r}")

    try:
        scan_date = parse_date(tool.get("scan_date"))
    except Exception as exc:
        fail(errors, f"{name}: invalid scan_date: {exc}")
        scan_date = None

    max_age_days = args.max_age_days
    if max_age_days is None:
        max_age_days = evidence.get("max_age_days")
    if scan_date and max_age_days is not None:
        age_days = (today - scan_date).days
        if age_days < 0:
            fail(errors, f"{name}: scan_date is in the future: {tool.get('scan_date')}")
        elif age_days > int(max_age_days):
            fail(errors, f"{name}: DAST evidence is stale: {age_days} days old, max is {max_age_days}")

    summary = tool.get("summary")
    if not isinstance(summary, dict):
        fail(errors, f"{name}: summary object is required")
        summary = {}

    for severity in evidence.get("release_criteria", {}).get("block_on_severities", []):
        field = SEVERITY_FIELDS.get(severity)
        if field and int(summary.get(field, 0)) > 0:
            fail(errors, f"{name}: blocking {severity} findings present: {summary.get(field)}")

    validate_report_files(repo_root, tool, errors, args.require_report_files)


def main():
    parser = argparse.ArgumentParser(description="Validate release DAST evidence manifest.")
    parser.add_argument("--evidence", default="release/dast-evidence.json")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--max-age-days", type=int, default=None)
    parser.add_argument("--require-report-files", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    evidence_path = (repo_root / args.evidence).resolve()
    errors = []

    with evidence_path.open(encoding="utf-8") as handle:
        evidence = json.load(handle)

    if evidence.get("version") != 1:
        fail(errors, "version must be 1")
    if not evidence.get("policy_name"):
        fail(errors, "policy_name is required")
    if not isinstance(evidence.get("tools"), list) or not evidence["tools"]:
        fail(errors, "tools must be a non-empty list")

    required_passed_tools = set(evidence.get("release_criteria", {}).get("required_passed_tools", []))
    tools_by_name = {tool.get("name"): tool for tool in evidence.get("tools", [])}
    for required_tool in sorted(required_passed_tools):
        tool = tools_by_name.get(required_tool)
        if not tool:
            fail(errors, f"required DAST tool is missing: {required_tool}")
        elif tool.get("status") not in PASS_STATUSES:
            fail(errors, f"required DAST tool did not pass: {required_tool}")

    today = dt.datetime.now(dt.timezone.utc).date()
    for tool in evidence.get("tools", []):
        validate_tool(repo_root, tool, evidence, today, args, errors)

    if errors:
        print("DAST evidence validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    required = ", ".join(sorted(required_passed_tools)) or "none"
    print(f"DAST evidence OK: {evidence_path}")
    print(f"Required passed tools: {required}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
