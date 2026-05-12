#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import sys
from pathlib import Path


def fail(errors, message):
    errors.append(message)


def parse_metric(value):
    if ":" not in value:
        raise argparse.ArgumentTypeError("metric must be name:value")
    name, raw = value.split(":", 1)
    try:
        percent = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid metric percent: {raw}") from exc
    return name, percent


def active_exception(policy, lane_name, today):
    for exception in policy.get("exceptions", []):
        if exception.get("lane") != lane_name:
            continue
        if exception.get("status") != "approved":
            continue
        if not exception.get("approver") or not exception.get("reason"):
            continue
        expires = dt.date.fromisoformat(exception.get("expires", "1970-01-01"))
        if expires >= today:
            return exception
    return None


def main():
    parser = argparse.ArgumentParser(description="Validate release coverage thresholds.")
    parser.add_argument("--policy", default="release/coverage-threshold-policy.json")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--metric", action="append", type=parse_metric, default=[])
    parser.add_argument("--metadata-only", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    with (repo_root / args.policy).open(encoding="utf-8") as handle:
        policy = json.load(handle)

    errors = []
    if policy.get("version") != 1:
        fail(errors, "version must be 1")
    if not policy.get("policy_name"):
        fail(errors, "policy_name is required")
    lanes = {lane.get("name"): lane for lane in policy.get("lanes", [])}
    if not lanes:
        fail(errors, "policy must define at least one coverage lane")

    today = dt.datetime.now(dt.timezone.utc).date()
    for exception in policy.get("exceptions", []):
        try:
            expires = dt.date.fromisoformat(exception.get("expires", "1970-01-01"))
        except ValueError:
            fail(errors, f"exception {exception.get('id', '<missing>')} has invalid expires date")
            continue
        if expires < today:
            fail(errors, f"coverage exception expired: {exception.get('id', '<missing>')}")

    if not args.metadata_only:
        metrics = dict(args.metric)
        for lane_name, lane in lanes.items():
            if lane_name not in metrics:
                fail(errors, f"missing coverage metric: {lane_name}")
                continue
            actual = metrics[lane_name]
            minimum = float(lane.get("minimum_percent", 0))
            if actual < minimum and not active_exception(policy, lane_name, today):
                fail(errors, f"{lane_name} coverage {actual:.2f}% is below required {minimum:.2f}%")

    if errors:
        print("Coverage threshold validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"Coverage threshold policy OK: {repo_root / args.policy}")
    for name, value in args.metric:
        print(f"{name}: {value:.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
