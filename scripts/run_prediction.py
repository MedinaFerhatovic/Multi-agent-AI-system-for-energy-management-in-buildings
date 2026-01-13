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
from agents.prediction import prediction_node

PIPELINE_NAME = "monitor_predict_backfill"
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

    for b in buildings:
        # ✅ uzmi anchor iz progress (ako ne postoji, init na MAX(timestamp))
        with connect() as conn:
            anchor = get_or_init_anchor(conn, PIPELINE_NAME, b)

        state = make_state(b, anchor)
        state = data_monitor_node(state)
        state = prediction_node(state)

        print(f"\n=== {b} @ {state['timestamp']} ===")
        print("ANOMALIES:", len(state["anomalies"]))
        print("PREDICTIONS:", len(state["predictions"]))
        print("LOG:")
        for line in state["execution_log"]:
            print(" -", line)
        if state["errors"]:
            print("ERRORS:", state["errors"])
            continue  # ne pomjeraj anchor ako je fail

        # ✅ nakon uspješnog run-a pomjeri anchor 24h unazad za sljedeći put
        with connect() as conn:
            next_anchor = step_anchor_back(conn, PIPELINE_NAME, b, hours=STEP_HOURS)

        print(f"NEXT_ANCHOR (next run): {next_anchor}")
