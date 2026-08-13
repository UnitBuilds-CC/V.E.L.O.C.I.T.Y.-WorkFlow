#!/usr/bin/env python3
"""
merge_results.py — Merge per-engine benchmark JSON results into a unified file.

Usage:
    python3 merge_results.py <results-dir> <output-file>

Reads all *.json files in <results-dir>, merges them into a single JSON
structure keyed by engine name, and writes to <output-file>.
"""

import json
import os
import sys
from pathlib import Path


def load_result(filepath: str) -> dict:
    """Load a single benchmark result JSON file."""
    with open(filepath) as f:
        data = json.load(f)
    return data


def extract_engine_name(filename: str) -> str:
    """Extract engine name from filename like 'velocity-classic_smoke.json'."""
    name = Path(filename).stem
    # Remove profile suffix
    parts = name.rsplit("_", 1)
    return parts[0] if len(parts) > 1 else name


def merge_results(results_dir: str, output_file: str):
    """Merge all JSON results in a directory."""
    merged = {
        "engines": {},
        "metadata": {
            "results_dir": results_dir,
            "files_merged": 0,
        },
    }

    results_path = Path(results_dir)
    json_files = sorted(results_path.glob("*.json"))

    for jf in json_files:
        if jf.name.startswith("merged_"):
            continue

        engine = extract_engine_name(jf.name)
        try:
            data = load_result(str(jf))
            if engine not in merged["engines"]:
                merged["engines"][engine] = {"workloads": []}

            # Handle different result formats
            if isinstance(data, dict):
                if "workloads" in data:
                    merged["engines"][engine]["workloads"].extend(data["workloads"])
                elif "results" in data:
                    merged["engines"][engine]["workloads"].extend(data["results"])
                else:
                    merged["engines"][engine]["workloads"].append(data)

            merged["metadata"]["files_merged"] += 1
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[merge] Warning: skipping {jf.name}: {e}", file=sys.stderr)

    with open(output_file, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"[merge] Merged {merged['metadata']['files_merged']} files → {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results-dir> <output-file>")
        sys.exit(1)

    merge_results(sys.argv[1], sys.argv[2])
