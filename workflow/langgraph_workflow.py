from __future__ import annotations

from datetime import datetime, timezone
from langgraph.graph import StateGraph, END

from workflow.state_schema import GraphState
from agents.data_monitor import data_monitor_node
from agents.prediction import prediction_node
from agents.optimization import optimization_node
from agents.decision import decision_node


def build_graph():
    workflow = StateGraph(GraphState)

    # Add nodes
    workflow.add_node("data_monitor", data_monitor_node)
    workflow.add_node("prediction", prediction_node)
    workflow.add_node("optimization", optimization_node)
    workflow.add_node("executive", decision_node)

    # Define edges
    workflow.set_entry_point("data_monitor")
    workflow.add_edge("data_monitor", "prediction")
    workflow.add_edge("prediction", "optimization")
    workflow.add_edge("optimization", "executive")
    workflow.add_edge("executive", END)

    # Compile
    return workflow.compile()


def make_initial_state(building_id: str) -> GraphState:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
