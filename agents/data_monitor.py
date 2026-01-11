from workflow.state_schema import GraphState
from utils.db_helper import connect, get_latest_readings, insert_anomalies
from utils.validators import validate_readings


def data_monitor_node(state: GraphState) -> GraphState:
    try:
        with connect() as conn:
            raw = get_latest_readings(conn, state["building_id"])
            validated, anomalies = validate_readings(raw)

            for a in anomalies:
                a["building_id"] = state["building_id"]

            insert_anomalies(conn, anomalies)

        state["sensor_data"] = raw
        state["validated_data"] = validated
        state["anomalies"] = anomalies
        state["execution_log"].append(
            f"DataMonitor: units={len(raw)}, anomalies={len(anomalies)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
