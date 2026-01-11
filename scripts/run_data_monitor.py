import sys
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from agents.data_monitor import data_monitor_node

state = {
    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "building_id": "B001",

    "sensor_data": {},
    "validated_data": {},
    "anomalies": [],

    "predictions": {},
    "optimization_plans": {},
    "final_decisions": [],

    "execution_log": [],
    "errors": [],
}

out = data_monitor_node(state)

print("LOG:", out["execution_log"])
print("ERRORS:", out["errors"])
print("ANOMALIES:", len(out["anomalies"]))
