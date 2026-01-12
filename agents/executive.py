from __future__ import annotations

from typing import Dict, Any, List
from datetime import datetime, timezone

from workflow.state_schema import GraphState
from utils.db_helper import connect, insert_decision_log


def _calc_confidence(plan: Dict[str, Any], pred: Dict[str, Any]) -> float:
    """
    Simplified "Bayesian-like" confidence:
    - base = model confidence (0..1) from predictions (your test R2 clamped)
    - penalize aggressive action when savings are tiny or consumption is low
    """
    base = float(pred.get("confidence") or 0.5)

    action = plan.get("action") or plan.get("action_type") or "maintain"
    pred_cons = float(pred.get("consumption") or 0.0)

    est_sav = plan.get("estimated_savings")
    est_sav = float(est_sav) if est_sav is not None else 0.0

    # if action is "reduce_heating" but predicted consumption is already low -> less confident
    penalty = 0.0
    if action == "reduce_heating":
        if pred_cons < 0.5:
            penalty += 0.15
        if est_sav < 0.02:
            penalty += 0.10

    conf = base - penalty
    if conf < 0.0:
        conf = 0.0
    if conf > 1.0:
        conf = 1.0
    return conf


def executive_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]
        now_ts = state.get("timestamp") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        decisions: List[Dict[str, Any]] = []

        with connect() as conn:
            for unit_id, plan in state.get("optimization_plans", {}).items():
                pred = state.get("predictions", {}).get(unit_id, {})

                conf = _calc_confidence(plan, pred)

                # normalize action field
                plan_action = plan.get("action") or plan.get("action_type") or "maintain"

                if conf > 0.7:
                    decision = {
                        "timestamp": now_ts,
                        "building_id": building_id,
                        "unit_id": unit_id,
                        "action": plan_action,
                        "approved": True,
                        "reasoning": f"High confidence ({conf:.2f})",
                        "confidence": conf,
                        "mode": "learning",
                    }
                else:
                    decision = {
                        "timestamp": now_ts,
                        "building_id": building_id,
                        "unit_id": unit_id,
                        "action": "maintain",
                        "approved": False,
                        "reasoning": f"Low confidence ({conf:.2f})",
                        "confidence": conf,
                        "mode": "learning",
                    }

                decisions.append(decision)
                insert_decision_log(conn, decision)

        state["final_decisions"] = decisions
        state["execution_log"].append(f"Executive: decisions={len(decisions)} approved={sum(1 for d in decisions if d.get('approved'))}")
    except Exception as e:
        state["errors"].append(str(e))

    return state
