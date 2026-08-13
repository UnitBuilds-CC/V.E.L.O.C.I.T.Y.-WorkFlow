#!/usr/bin/env python3
"""Check DBOS API and test basic functionality."""
from dbos import DBOS, DBOSConfig
import inspect

# Print all public methods/attributes
methods = [m for m in dir(DBOS) if not m.startswith('_')]
print("DBOS public methods:")
for m in methods:
    print(f"  {m}")

# Check if key methods exist
print("\nKey method checks:")
print(f"  workflow: {hasattr(DBOS, 'workflow')}")
print(f"  transaction: {hasattr(DBOS, 'transaction')}")
print(f"  step: {hasattr(DBOS, 'step')}")
print(f"  start_workflow: {hasattr(DBOS, 'start_workflow')}")
print(f"  launch: {hasattr(DBOS, 'launch')}")
print(f"  get_kv: {hasattr(DBOS, 'get_kv')}")
print(f"  set_kv: {hasattr(DBOS, 'set_kv')}")
print(f"  kafka: {hasattr(DBOS, 'kafka')}")
print(f"  scheduler: {hasattr(DBOS, 'scheduler')}")
print(f"  event: {hasattr(DBOS, 'event')}")
print(f"  recv: {hasattr(DBOS, 'recv')}")
print(f"  set_event: {hasattr(DBOS, 'set_event')}")
print(f"  get_event: {hasattr(DBOS, 'get_event')}")

# Check DBOSConfig
print("\nDBOSConfig keys:")
import typing
if hasattr(DBOSConfig, '__annotations__'):
    for k, v in DBOSConfig.__annotations__.items():
        print(f"  {k}: {v}")
