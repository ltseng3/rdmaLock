#!/usr/bin/env python3
"""Extract only fully verified lock-benchmark runs into CSV.

Usage: python3 tools/apt/e2e_results.py run*.log > results.csv
"""
import argparse
import csv
import pathlib
import re
import sys

FIELDS = ["run", "positions", "workers", "completed", "violations",
          "verified_markers", "ordered_entries", "probe", "work_ns",
          "ordering_seconds", "handoff_seconds", "goodput_ops_per_sec",
          "including_ordering_ops_per_sec", "status"]


def extract(text: str):
    lines = [line.strip() for line in text.splitlines()
             if line.strip().startswith("LOCK_E2E ")]
    if len(lines) != 1:
        raise ValueError("expected exactly one LOCK_E2E summary line")
    pairs = dict(re.findall(r"([a-z_]+)=([^\s]+)", lines[0]))
    for key in FIELDS[1:]:
        if key not in pairs:
            raise ValueError("missing field " + key)
    if pairs["status"] != "PASS":
        raise ValueError("run did not pass")
    if int(pairs["completed"]) != int(pairs["positions"]):
        raise ValueError("partial run")
    if int(pairs["violations"]) or int(pairs["verified_markers"]) != int(pairs["positions"]):
        raise ValueError("failed safety verification")
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=pathlib.Path)
    args = parser.parse_args()
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS)
    writer.writeheader()
    for path in args.logs:
        try:
            d = extract(path.read_text())
            writer.writerow({"run": path.name, **d})
        except (OSError, ValueError) as exc:
            print(f"REJECT {path}: {exc}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
