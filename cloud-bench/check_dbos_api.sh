#!/bin/bash
echo "=== DBOS API check ==="
python3 << 'PYEOF'
import dbos
print("DBOS attributes:", [x for x in dir(dbos) if not x.startswith('_')])
print()
# Check if DBOS class exists and its methods
if hasattr(dbos, 'DBOS'):
    d = dbos.DBOS
    print("DBOS class methods:", [x for x in dir(d) if not x.startswith('_')])
else:
    print("No DBOS class found")
    # Check for alternative API
    for name in dir(dbos):
        obj = getattr(dbos, name)
        if callable(obj) and not name.startswith('_'):
            print(f"  {name}: {type(obj)}")
PYEOF
echo "=== DONE ==="
