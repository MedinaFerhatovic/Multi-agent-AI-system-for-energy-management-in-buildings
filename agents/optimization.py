# agents/optimization.py
from __future__ import annotations

from typing import Dict, Any, List, Optional
from datetime import timezone, datetime

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    get_tariff_for_building,
    get_price_for_timestamp,
    get_unit_cluster,
    insert_optimization_plans,
)

# Comfort / setback pragovi (možeš kasnije fino naštimati)
OCC_EMPTY_TH = 0.20     # ispod ovoga tretiramo kao "prazno"
OCC_PRESENT_TH = 0.60   # iznad ovoga tretiramo kao "ima ljudi"

# Temperature setpoints
TEMP_COMFORT = 21.0
TEMP_SETBACK = 17.0        # kad je vjerovatno prazno
TEMP_REDUCE_HIGH_TARIFF = 19.0

# Energy threshold (kWh po intervalu) kad je high-tariff -> agresivnije
DEFAULT_CONSUMPTION_THRESHOLD = 1.2

# Policy defaults (energy-first)
DEFAULT_POLICY = {
    "cost_weight": 1.0,
    "comfort_weight": 0.8,
    "stability_weight": 0.4,
}


def get_priority_for_cluster(cluster_id: Optional[str]) -> float:
    """
    Cluster -> prioritet. Veći prioritet => ranije reaguješ (niži prag).
    Za energy-management logičnije je da "vacant" dobije veći prioritet štednje.
    """
    if not cluster_id:
        return 1.0

    c = cluster_id.lower()
    if "vacant" in c or "minimal" in c:
        return 1.5  # ✅ prazno -> agresivnije štednje
    if "commercial" in c:
        return 1.2
    if "high" in c:
        return 1.1
    if "low" in c:
        return 0.9
    return 1.0


def estimate_savings(pred_kwh: float, price_per_kwh: float, factor: float) -> float:
    """
    Jednostavna heuristika: ušteda = pred_kwh * factor * price.
    factor npr: 0.1 (10%) ili 0.2 (20%)
    """
    if pred_kwh is None:
        return 0.0
    return round(float(pred_kwh) * float(factor) * float(price_per_kwh), 4)


def optimization_node(state: GraphState) -> GraphState:
    """
    Agent 3 (Optimization/Planning) - OFFLINE/ANCHOR aware:
    - koristi state["timestamp"] kao anchor (ne now())
    - koristi prediction.timestamp_target za cijenu tarife
    - koristi predicted_occupancy_prob za comfort/setback
    - upisuje planove u optimization_plans + state["optimization_plans"]
    """
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]  # ✅ offline anchor

        preds: Dict[str, Any] = state.get("predictions") or {}
        policy = state.get("policy") or DEFAULT_POLICY
        if not preds:
            state["execution_log"].append(f"Optimization(v2): skipped (no predictions) anchor={anchor_ts}")
            return state

        plans: Dict[str, Any] = {}
        rows_for_db: List[Dict[str, Any]] = []

        with connect() as conn:
            tariff = get_tariff_for_building(conn, building_id)
            high_price = float(tariff["high_price_per_kwh"])

            for unit_id, pred in preds.items():
                # --- predicted consumption (kWh per interval) ---
                pred_cons = pred.get("predicted_consumption", pred.get("consumption"))
                if pred_cons is None:
                    continue
                pred_cons = float(pred_cons)

                # --- target timestamp from prediction ---
                ts_target = pred.get("timestamp_target") or anchor_ts

                # --- price at target time (not at anchor) ---
                price = float(get_price_for_timestamp(tariff, ts_target))

                # --- occupancy probability ---
                occ_prob = pred.get("predicted_occupancy_prob")
                occ_prob_f = None if occ_prob is None else float(occ_prob)

                # --- cluster priority (optional) ---
                cluster_id = get_unit_cluster(conn, building_id, unit_id)
                priority = get_priority_for_cluster(cluster_id)

                # Adjust threshold: bigger priority => lower threshold => react earlier
                threshold = DEFAULT_CONSUMPTION_THRESHOLD / max(priority, 0.1)

                # ---- choose action (energy-first) ----
                # default: maintain comfort
                action_type = "maintain"
                target_temp = TEMP_COMFORT
                savings_factor = 0.0
                reason = []

                # 1) If likely empty -> setback (biggest energy lever)
                if occ_prob_f is not None and occ_prob_f < OCC_EMPTY_TH:
                    action_type = "setback_unoccupied"
                    target_temp = TEMP_SETBACK
                    savings_factor = 0.20 * float(policy.get("cost_weight", 1.0))
                    reason.append(f"occ_prob<{OCC_EMPTY_TH}")

                # 2) If high tariff + high predicted consumption -> reduce heating
                # (only if not already empty-setback; or you can allow even stronger action)
                if action_type != "setback_unoccupied":
                    if price >= high_price and pred_cons > threshold:
                        action_type = "reduce_heating_high_tariff"
                        target_temp = TEMP_REDUCE_HIGH_TARIFF
                        savings_factor = 0.10 * float(policy.get("cost_weight", 1.0))
                        reason.append("high_tariff_and_high_pred")

                # estimated cost/savings (interval-level)
                estimated_cost = round(pred_cons * price, 4)
                estimated_savings = estimate_savings(pred_cons, price, savings_factor)

                plan = {
                    "timestamp_target": ts_target,
                    "action": action_type,
                    "target_temp": target_temp,
                    "predicted_kwh_interval": round(pred_cons, 3),
                    "price_per_kwh": round(price, 4),
                    "estimated_cost": estimated_cost,
                    "estimated_savings": estimated_savings,
                    "predicted_occupancy_prob": occ_prob_f,
                    "cluster_id": cluster_id,
                    "priority": priority,
                    "reason": ";".join(reason) if reason else None,
                    "risk": 0.25 if action_type != "maintain" else 0.05,
                }
                plans[unit_id] = plan

                rows_for_db.append({
                    # in DB schema optimization_plans.timestamp is "timestamp"
                    "timestamp": anchor_ts,  # ✅ deterministic/offline
                    "building_id": building_id,
                    "unit_id": unit_id,
                    "action_type": action_type,
                    "target_temp": target_temp,
                    # optional scheduling windows (you can set to ts_target..ts_target+interval later)
                    "start_time": ts_target,
                    "end_time": None,
                    "estimated_cost": estimated_cost,
                    "estimated_savings": estimated_savings,
                    "method": "heuristic_v2",
                })

            insert_optimization_plans(conn, rows_for_db)

        state["optimization_plans"] = plans
        state["execution_log"].append(
            f"Optimization(v2): anchor={anchor_ts} plans={len(plans)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
