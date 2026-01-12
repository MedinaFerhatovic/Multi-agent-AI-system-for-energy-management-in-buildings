import sys
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter  # ✅ Counter je ovdje

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from utils.db_helper import connect, get_all_building_ids, get_latest_timestamp
from agents.data_monitor import data_monitor_node

with connect() as conn:
    buildings = get_all_building_ids(conn)

for bid in buildings:
    with connect() as conn:
        anchor = get_latest_timestamp(conn, bid)  # offline “now”

    state = {
        "timestamp": anchor or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "building_id": bid,
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

    print(f"\n=== {bid} @ {state['timestamp']} ===")
    print("EVENTS:", len(out["anomalies"]))
    print("BY_CATEGORY:", {
        "data_quality": sum(1 for a in out["anomalies"] if a.get("category") == "data_quality"),
        "operational": sum(1 for a in out["anomalies"] if a.get("category") == "operational"),
    })

    print("EXECUTION_LOG:")
    for line in out["execution_log"]:
        print(" -", line)

    types = Counter(a.get("type") for a in out["anomalies"])
    print("EVENT_TYPES:", dict(types))

    for a in out["anomalies"][:10]:
        print(a.get("timestamp"), a.get("unit_id"), a.get("type"), a.get("value"), a.get("details"))

    if out["errors"]:
        print("ERRORS:", out["errors"])
