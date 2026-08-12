import json
d = json.load(open("/home/ian_unitbuilds_com/http_bench_results.json"))
print("=== Restate results ===")
for r in d["restate_results"][:8]:
    print(f"  {r['workload_name']}: success={r['successful_operations']} failed={r['failed_operations']} ops/sec={r['operations_per_second']}")
print("\n=== Velocity results ===")
for r in d["velocity_results"][:8]:
    print(f"  {r['workload_name']}: success={r['successful_operations']} failed={r['failed_operations']} ops/sec={r['operations_per_second']}")
