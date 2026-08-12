#!/usr/bin/env python3
"""
cloud-bench/aggregate_results.py

Reads JSON result files from all 6 VMs and produces:
  - benchmark_comparison.json  — unified data
  - benchmark_comparison.md    — markdown report with tables
  - Summary stats: Velocity wins/losses per flavor pair, avg speedup

Usage:
  python3 aggregate_results.py --input-dir ./results --output ./results/aggregated
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def load_json_results(input_dir: str) -> dict:
    """Load all JSON result files from subdirectories."""
    results = {}
    input_path = Path(input_dir)

    # Map subdirectory names to flavor pairs
    flavor_map = {
        "classic": {"velocity": None, "competitor": None, "competitor_name": "Temporal", "pair": "Classic (gRPC)"},
        "temporal": {"velocity": None, "competitor": None, "competitor_name": "Temporal", "pair": "Classic (gRPC)"},
        "runtime": {"velocity": None, "competitor": None, "competitor_name": "Restate", "pair": "Runtime (HTTP)"},
        "restate": {"velocity": None, "competitor": None, "competitor_name": "Restate", "pair": "Runtime (HTTP)"},
        "embedded": {"velocity": None, "competitor": None, "competitor_name": "DBOS", "pair": "Embedded (Postgres)"},
        "dbos": {"velocity": None, "competitor": None, "competitor_name": "DBOS", "pair": "Embedded (Postgres)"},
    }

    for flavor_dir in input_path.iterdir():
        if not flavor_dir.is_dir():
            continue

        flavor = flavor_dir.name
        if flavor not in flavor_map:
            continue

        # Find JSON files in this directory
        json_files = list(flavor_dir.glob("*.json"))
        if not json_files:
            print(f"  Warning: No JSON files in {flavor_dir}", file=sys.stderr)
            continue

        # Load the first (or only) JSON file
        with open(json_files[0]) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"  Warning: Invalid JSON in {json_files[0]}: {e}", file=sys.stderr)
                continue

        # Classify as velocity or competitor
        is_velocity = any(kw in flavor for kw in ["velocity", "classic", "runtime", "embedded"])
        if is_velocity:
            flavor_map[flavor]["velocity"] = data
        else:
            flavor_map[flavor]["competitor"] = data

    # Merge paired results
    pairs = {}
    seen_pairs = set()
    for flavor, info in flavor_map.items():
        pair_name = info["pair"]
        if pair_name in seen_pairs:
            continue
        seen_pairs.add(pair_name)
        pairs[pair_name] = info

    return pairs


def extract_workload_results(data: dict) -> list:
    """Extract per-workload results from a benchmark JSON."""
    results = []

    # gRPC benchmark format (velocity-bench)
    if "workloads" in data:
        for w in data["workloads"]:
            results.append({
                "name": w.get("workload", w.get("name", "unknown")),
                "velocity_ops_sec": w.get("velocity_ops_per_second", 0),
                "competitor_ops_sec": w.get("temporal_ops_per_second", w.get("competitor_ops_per_second", 0)),
                "velocity_p99_us": w.get("velocity_p99_us", w.get("velocity_latency_p99_us", 0)),
                "competitor_p99_us": w.get("temporal_p99_us", w.get("competitor_latency_p99_us", 0)),
                "velocity_mem_mb": w.get("velocity_peak_memory_mb", 0),
                "competitor_mem_mb": w.get("temporal_peak_memory_mb", w.get("competitor_peak_memory_mb", 0)),
            })

    # HTTP benchmark format
    elif "results" in data:
        for w in data["results"]:
            results.append({
                "name": w.get("workload_name", "unknown"),
                "velocity_ops_sec": w.get("velocity_ops_per_second", 0),
                "competitor_ops_sec": w.get("restate_ops_per_second", 0),
                "velocity_p99_us": w.get("velocity_latency_p99_us", 0),
                "competitor_p99_us": w.get("restate_latency_p99_us", 0),
                "velocity_mem_mb": w.get("velocity_peak_memory_mb", 0),
                "competitor_mem_mb": w.get("restate_peak_memory_mb", 0),
            })

    # Embedded benchmark format
    elif "sequential" in data:
        results.append({
            "name": "sequential_workflow",
            "velocity_ops_sec": data.get("sequential", {}).get("operations_per_second", 0),
            "competitor_ops_sec": 0,  # filled in by pair merge
            "velocity_p99_us": data.get("sequential", {}).get("latency_p99_ms", 0) * 1000,
            "competitor_p99_us": 0,
            "velocity_mem_mb": data.get("memory", {}).get("engine_after_mb", 0),
            "competitor_mem_mb": 0,
        })
        results.append({
            "name": "concurrent_workflow",
            "velocity_ops_sec": data.get("concurrent", {}).get("operations_per_second", 0),
            "competitor_ops_sec": 0,
            "velocity_p99_us": 0,
            "competitor_p99_us": 0,
            "velocity_mem_mb": data.get("memory", {}).get("engine_after_mb", 0),
            "competitor_mem_mb": 0,
        })
        results.append({
            "name": "sustained_load",
            "velocity_ops_sec": data.get("sustained", {}).get("operations_per_second", 0),
            "competitor_ops_sec": 0,
            "velocity_p99_us": data.get("sustained", {}).get("latency_p99_ms", 0) * 1000,
            "competitor_p99_us": 0,
            "velocity_mem_mb": data.get("memory", {}).get("engine_after_mb", 0),
            "competitor_mem_mb": 0,
        })
        # pgbench baseline
        if "pgbench" in data and data["pgbench"].get("tps"):
            results.append({
                "name": "pgbench_raw_tps",
                "velocity_ops_sec": data["pgbench"].get("tps", 0),
                "competitor_ops_sec": 0,
                "velocity_p99_us": data["pgbench"].get("latency_p99_ms", 0) * 1000,
                "competitor_p99_us": 0,
                "velocity_mem_mb": data.get("memory", {}).get("postgres_after_mb", 0),
                "competitor_mem_mb": 0,
            })

    return results


def compute_speedup(vel: float, comp: float) -> float:
    """Compute speedup ratio. >1.0 means Velocity wins."""
    if comp <= 0 or vel <= 0:
        return 0.0
    return vel / comp


def generate_markdown(pairs: dict, output_path: str):
    """Generate markdown comparison report."""
    lines = []
    lines.append("# 3-Flavor Cloud Benchmark Results")
    lines.append("")
    lines.append(f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    total_velocity_wins = 0
    total_velocity_losses = 0
    total_speedups = []

    for pair_name, info in pairs.items():
        vel_data = info.get("velocity")
        comp_data = info.get("competitor")
        comp_name = info.get("competitor_name", "Competitor")

        lines.append(f"## {pair_name}: Velocity vs {comp_name}")
        lines.append("")

        if not vel_data and not comp_data:
            lines.append("*No results available for this pair.*")
            lines.append("")
            continue

        # Extract workload results
        vel_results = extract_workload_results(vel_data) if vel_data else []
        comp_results = extract_workload_results(comp_data) if comp_data else []

        # Merge by workload name
        workload_names = list(dict.fromkeys(
            [r["name"] for r in vel_results] + [r["name"] for r in comp_results]
        ))

        vel_by_name = {r["name"]: r for r in vel_results}
        comp_by_name = {r["name"]: r for r in comp_results}

        # Table header
        lines.append("| Workload | Velocity (ops/s) | {} (ops/s) | Speedup | Velocity p99 | {} p99 |".format(
            comp_name, comp_name))
        lines.append("|----------|-----------------|-------------|---------|-------------|--------|")

        pair_wins = 0
        pair_losses = 0
        pair_speedups = []

        for wname in workload_names:
            vel_r = vel_by_name.get(wname, {})
            comp_r = comp_by_name.get(wname, {})

            vel_ops = vel_r.get("velocity_ops_sec", 0)
            comp_ops = comp_r.get("competitor_ops_sec", 0)

            # For competitor results from paired data, use competitor_ops_sec
            if not comp_ops and comp_r:
                comp_ops = comp_r.get("velocity_ops_sec", 0)  # competitor's own result

            speedup = compute_speedup(vel_ops, comp_ops)

            vel_p99 = vel_r.get("velocity_p99_us", 0)
            comp_p99 = comp_r.get("competitor_p99_us", 0)
            if not comp_p99 and comp_r:
                comp_p99 = comp_r.get("velocity_p99_us", 0)

            if speedup > 1.0:
                winner = "**✓**"
                pair_wins += 1
            elif speedup < 1.0 and speedup > 0:
                winner = "✗"
                pair_losses += 1
            else:
                winner = "—"

            lines.append(f"| {wname} | {vel_ops:,.0f} | {comp_ops:,.0f} | {speedup:.2f}x {winner} | {vel_p99:,.0f}µs | {comp_p99:,.0f}µs |")

            if speedup > 0:
                pair_speedups.append(speedup)

        lines.append("")

        if pair_speedups:
            avg_speedup = sum(pair_speedups) / len(pair_speedups)
            lines.append(f"**{pair_name} Summary:** Velocity wins {pair_wins}/{pair_wins + pair_losses} workloads, avg speedup {avg_speedup:.2f}x")
        else:
            lines.append(f"**{pair_name} Summary:** {pair_wins} wins, {pair_losses} losses")

        lines.append("")

        total_velocity_wins += pair_wins
        total_velocity_losses += pair_losses
        total_speedups.extend(pair_speedups)

    # Overall summary
    lines.append("---")
    lines.append("")
    lines.append("## Overall Summary")
    lines.append("")

    if total_speedups:
        avg = sum(total_speedups) / len(total_speedups)
        lines.append(f"- **Total Velocity Wins:** {total_velocity_wins}/{total_velocity_wins + total_velocity_losses}")
        lines.append(f"- **Average Speedup:** {avg:.2f}x")
        lines.append(f"- **Flavor Pairs Tested:** {len(pairs)}")
    else:
        lines.append("- No comparable results available for summary.")

    lines.append("")

    # Write file
    md_path = os.path.join(output_path, "benchmark_comparison.md")
    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    print(f"  Written: {md_path}")
    return md_path


def generate_json(pairs: dict, output_path: str):
    """Generate unified JSON comparison."""
    unified = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pairs": {},
    }

    for pair_name, info in pairs.items():
        unified["pairs"][pair_name] = {
            "velocity": info.get("velocity"),
            "competitor": info.get("competitor"),
            "competitor_name": info.get("competitor_name"),
        }

    json_path = os.path.join(output_path, "benchmark_comparison.json")
    with open(json_path, "w") as f:
        json.dump(unified, f, indent=2, default=str)

    print(f"  Written: {json_path}")
    return json_path


def main():
    parser = argparse.ArgumentParser(description="Aggregate 3-flavor benchmark results")
    parser.add_argument("--input-dir", required=True, help="Directory containing per-flavor result subdirectories")
    parser.add_argument("--output", required=True, help="Output directory for aggregated results")
    args = parser.parse_args()

    print(f"Aggregating results from: {args.input_dir}")

    # Create output directory
    os.makedirs(args.output, exist_ok=True)

    # Load results
    pairs = load_json_results(args.input_dir)
    print(f"  Found {len(pairs)} flavor pairs")

    for pair_name, info in pairs.items():
        vel = "✓" if info.get("velocity") else "✗"
        comp = "✓" if info.get("competitor") else "✗"
        print(f"    {pair_name}: Velocity {vel}, {info['competitor_name']} {comp}")

    # Generate outputs
    generate_json(pairs, args.output)
    generate_markdown(pairs, args.output)

    print("")
    print("Aggregation complete.")


if __name__ == "__main__":
    main()
