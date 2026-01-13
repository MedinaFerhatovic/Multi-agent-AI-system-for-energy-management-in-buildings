import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from utils.db_helper import (
    connect,
    get_all_building_ids,
    ensure_pipeline_progress,
    get_or_init_anchor,
    step_anchor_back,
)
from agents.data_monitor import data_monitor_node

PIPELINE_NAME = "data_monitor_backfill"
STEP_HOURS = 24

def make_state(building_id: str, anchor_ts: str):
    return {
        "timestamp": anchor_ts,
        "building_id": building_id,
        "sensor_data": {},
        "validated_data": {},
        "anomalies": [],
        "predictions": {},
        "optimization_plans": {},
        "final_decisions": [],
        "validation_report": {},
        "policy": {},
        "execution_log": [],
        "errors": [],
    }

if __name__ == "__main__":
    with connect() as conn:
        ensure_pipeline_progress(conn)
        buildings = get_all_building_ids(conn)

    for bid in buildings:
        with connect() as conn:
            anchor = get_or_init_anchor(conn, PIPELINE_NAME, bid)

        state = make_state(bid, anchor)
        out = data_monitor_node(state)

        print(f"\n=== {bid} @ {anchor} ===")
        print("ANOMALIES:", len(out["anomalies"]))
        print("LOG:")
        for line in out["execution_log"]:
            print(" -", line)
        if out["errors"]:
            print("ERRORS:", out["errors"])
            continue 

        with connect() as conn:
            next_anchor = step_anchor_back(conn, PIPELINE_NAME, bid, hours=STEP_HOURS)

        print(f"NEXT_ANCHOR (next run): {next_anchor}")
