#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import sys
from pathlib import Path


def parse_date(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def fail(errors, message):
    errors.append(message)


def main():
    parser = argparse.ArgumentParser(description="Validate backup/restore production policy evidence.")
    parser.add_argument("--policy", default="release/backup-restore-policy.json")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--max-age-days", type=int, default=None)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    policy_path = repo_root / args.policy
    with policy_path.open(encoding="utf-8") as handle:
        policy = json.load(handle)

    errors = []
    if policy.get("version") != 1:
        fail(errors, "version must be 1")
    if not policy.get("policy_name"):
        fail(errors, "policy_name is required")

    backup_policy = policy.get("backup_policy", {})
    if backup_policy.get("encrypted_backups_required") is not True:
        fail(errors, "encrypted_backups_required must be true")
    if backup_policy.get("strict_startup_guard_required") is not True:
        fail(errors, "strict_startup_guard_required must be true")
    if backup_policy.get("required_key_algorithm") != "AES-256-GCM":
        fail(errors, "required_key_algorithm must be AES-256-GCM")
    if int(backup_policy.get("required_key_bytes", 0)) != 32:
        fail(errors, "required_key_bytes must be 32")

    restore_policy = policy.get("restore_policy", {})
    for field in [
        "restore_requires_import_gate",
        "restore_limits_required",
        "checksum_sidecar_required",
        "live_preprod_restore_drill_required_for_release_candidate",
    ]:
        if restore_policy.get(field) is not True:
            fail(errors, f"{field} must be true")

    evidence = policy.get("evidence", {})
    startup_guard = evidence.get("startup_guard", {})
    restore_validation = evidence.get("restore_validation", {})
    if startup_guard.get("status") != "implemented":
        fail(errors, "startup guard evidence must be implemented")
    if restore_validation.get("status") != "implemented":
        fail(errors, "restore validation evidence must be implemented")
    if len(startup_guard.get("tests", [])) < 2:
        fail(errors, "startup guard evidence must list tests")
    if len(restore_validation.get("tests", [])) < 3:
        fail(errors, "restore validation evidence must list restore tests")

    for section in [startup_guard, restore_validation]:
        for file_name in section.get("files", []):
            if not (repo_root / file_name).exists():
                fail(errors, f"evidence file is missing: {file_name}")

    latest = evidence.get("latest_verification", {})
    if latest.get("status") != "pass":
        fail(errors, "latest verification status must be pass")
    if int(latest.get("passed_tests", 0)) <= 0:
        fail(errors, "latest verification must record passed test count")

    max_age_days = args.max_age_days if args.max_age_days is not None else policy.get("max_age_days")
    if max_age_days is not None:
        verification_date = parse_date(latest.get("date", "1970-01-01"))
        age_days = (dt.datetime.now(dt.timezone.utc).date() - verification_date).days
        if age_days < 0:
            fail(errors, "latest verification date is in the future")
        elif age_days > int(max_age_days):
            fail(errors, f"backup/restore policy evidence is stale: {age_days} days old, max is {max_age_days}")

    if errors:
        print("Backup/restore policy validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"Backup/restore policy OK: {policy_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
