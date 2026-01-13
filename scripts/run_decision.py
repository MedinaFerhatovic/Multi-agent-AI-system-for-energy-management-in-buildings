# scripts/run_full_backfill.py
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
from agents.optimization import optimization_node
from agents.decision import decision_node
from scripts import feature_extractor, clustering

PIPELINE_NAME = "full_pipeline_backfill"
STEP_HOURS = 24
RUN_FEATURES_AND_CLUSTERING = True

DB_PATH = BASE_DIR / "db" / "smartbuilding.db"

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

        if RUN_FEATURES_AND_CLUSTERING:
            feature_extractor.run(str(DB_PATH), bid)
            clustering.run(str(DB_PATH), bid, n_clusters=None)

        state = make_state(bid, anchor)

        state = data_monitor_node(state)
        state = prediction_node(state)
        state = optimization_node(state)
        state = decision_node(state)

        print(f"\n=== {bid} @ {anchor} ===")
        print("ANOMALIES:", len(state["anomalies"]))
        print("PREDICTIONS:", len(state["predictions"]))
        print("PLANS:", len(state["optimization_plans"]))
        print("DECISIONS:", len(state["final_decisions"]))
        print("LOG:")
        for line in state["execution_log"]:
            print(" -", line)
        if state["errors"]:
            print("ERRORS:", state["errors"])
            continue

        with connect() as conn:
            next_anchor = step_anchor_back(conn, PIPELINE_NAME, bid, hours=STEP_HOURS)

        print(f"NEXT_ANCHOR (next run): {next_anchor}")
