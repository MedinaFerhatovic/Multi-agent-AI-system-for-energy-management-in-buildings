import sys
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from utils.db_helper import connect
from agents.data_monitor import data_monitor_node
from agents.prediction import prediction_node
from agents.optimization import optimization_node


def get_buildings(conn):
    rows = conn.execute("SELECT building_id FROM buildings ORDER BY building_id").fetchall()
    return [r[0] for r in rows]


def make_state(building_id: str):
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "building_id": building_id,

        "sensor_data": {},
        "validated_data": {},
        "anomalies": [],

        "predictions": {},
        "optimization_plans": {},
        "final_decisions": [],

        "execution_log": [],
        "errors": [],
    }


if __name__ == "__main__":
    # prvo uzmi listu zgrada
    with connect() as conn:
        buildings = get_buildings(conn)

    # onda obradi jednu po jednu (smanjuje lock probleme)
    for b in buildings:
        state = make_state(b)

        state = data_monitor_node(state)
        state = prediction_node(state)
        state = optimization_node(state)

        print(f"\n=== {b} ===")
        print("LOG:", state["execution_log"])
        print("ERRORS:", state["errors"])
        print("ANOMALIES:", len(state["anomalies"]))
        print("PREDICTIONS:", len(state["predictions"]))
        print("PLANS:", len(state["optimization_plans"]))
