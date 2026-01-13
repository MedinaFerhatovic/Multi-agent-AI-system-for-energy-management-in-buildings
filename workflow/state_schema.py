from typing import TypedDict, Dict, List, Any


class GraphState(TypedDict):
    timestamp: str
    building_id: str

    sensor_data: Dict[str, Any]
    validated_data: Dict[str, Any]
    anomalies: List[Dict[str, Any]]

    predictions: Dict[str, Any]
    optimization_plans: Dict[str, Any]
    final_decisions: List[Dict[str, Any]]

    validation_report: Dict[str, Any]
    policy: Dict[str, Any]
    execution_log: List[str]
    errors: List[str]
