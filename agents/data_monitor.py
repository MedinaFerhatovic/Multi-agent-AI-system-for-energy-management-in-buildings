from workflow.state_schema import GraphState
from utils.db_helper import connect, get_latest_readings_asof, insert_anomalies
from utils.validators import validate_readings

def data_monitor_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]  # ✅ anchor iz state-a (offline)

        with connect() as conn:
            raw = get_latest_readings_asof(conn, building_id, anchor_ts)
            validated, anomalies = validate_readings(raw)

            for a in anomalies:
                a["building_id"] = building_id

            insert_anomalies(conn, anomalies)

        state["sensor_data"] = raw
        state["validated_data"] = validated
        state["anomalies"] = anomalies
        state["execution_log"].append(
            f"DataMonitor(off): building={building_id} anchor={anchor_ts} units={len(raw)} anomalies={len(anomalies)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
