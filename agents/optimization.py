from datetime import datetime, timezone
from typing import Dict, Any

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    get_tariff_for_building,
    get_price_for_timestamp,
    get_unit_cluster,
    insert_optimization_plans,
)

# Prag potrošnje (kWh) za “agresivniju” akciju
DEFAULT_CONSUMPTION_THRESHOLD = 1.2

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def get_priority_for_cluster(cluster_id: str | None) -> float:
    """
    Mali helper: cluster -> prioritet (1.0 normal).
    Možeš kasnije mapirati kako hoćeš.
    """
    if not cluster_id:
        return 1.0

    c = cluster_id.lower()
    if "high" in c:
        return 1.4
    if "commercial" in c:
        return 1.2
    if "vacant" in c or "minimal" in c:
        return 0.6
    if "low" in c:
        return 0.8
    return 1.0

def estimate_savings(pred_kwh: float, action_type: str, price_per_kwh: float) -> float:
    """
    Heuristika: ako smanjujemo grijanje, pretpostavi 10% uštede na predikciju.
    """
    if action_type != "reduce_heating":
        return 0.0
    return round(pred_kwh * 0.10 * price_per_kwh, 4)

def optimization_node(state: GraphState) -> GraphState:
    """
    Agent 3:
    - čita state['predictions']
    - iz tarife izračuna cijenu
    - napravi plan po unit-u
    - upiše u optimization_plans tabelu
    - napuni state['optimization_plans']
    """
    try:
        building_id = state["building_id"]
        ts_now = state.get("timestamp") or _now_utc_iso()

        preds: Dict[str, Any] = state.get("predictions") or {}
        if not preds:
            state["execution_log"].append("Optimization: skipped (no predictions)")
            return state

        plans: Dict[str, Any] = {}
        rows_for_db = []

        with connect() as conn:
            tariff = get_tariff_for_building(conn, building_id)
            price = get_price_for_timestamp(tariff, ts_now)
            high_price = float(tariff["high_price_per_kwh"])

            for unit_id, pred in preds.items():
                # kompatibilno: nekad je 'consumption', nekad 'predicted_consumption'
                pred_cons = pred.get("consumption", pred.get("predicted_consumption"))
                if pred_cons is None:
                    continue
                pred_cons = float(pred_cons)

                cluster_id = get_unit_cluster(conn, building_id, unit_id)
                priority = get_priority_for_cluster(cluster_id)

                threshold = DEFAULT_CONSUMPTION_THRESHOLD / max(priority, 0.1)

                if pred_cons > threshold and price >= high_price:
                    action_type = "reduce_heating"
                    target_temp = 19.0
                else:
                    action_type = "maintain"
                    target_temp = 21.0

                estimated_cost = round(pred_cons * price, 4)
                estimated_savings = estimate_savings(pred_cons, action_type, price)

                plan = {
                    "action": action_type,
                    "target_temp": target_temp,
                    "estimated_cost": estimated_cost,
                    "estimated_savings": estimated_savings,
                    "price_per_kwh": price,
                    "cluster_id": cluster_id,
                    "priority": priority,
                }
                plans[unit_id] = plan

                rows_for_db.append({
                    "timestamp": ts_now,
                    "building_id": building_id,
                    "unit_id": unit_id,
                    "action_type": action_type,
                    "target_temp": target_temp,
                    "start_time": None,
                    "end_time": None,
                    "estimated_cost": estimated_cost,
                    "estimated_savings": estimated_savings,
                    "method": "heuristic_v1",
                })

            insert_optimization_plans(conn, rows_for_db)

        state["optimization_plans"] = plans
        state["execution_log"].append(
            f"Optimization: plans={len(plans)} price={price}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
