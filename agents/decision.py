# agents/decision.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from datetime import timezone

from workflow.state_schema import GraphState
from utils.db_helper import connect, insert_decisions_rows


# thresholds (možeš fino naštimati)
CONF_APPROVE_TH = 0.60
OCC_HIGH_TH = 0.60
MIN_COMFORT_TEMP = 19.0


def _get_predicted_consumption(pred: Dict[str, Any]) -> float:
    return float(pred.get("predicted_consumption", pred.get("consumption", 0.0)) or 0.0)


def _get_plan_action(plan: Dict[str, Any]) -> str:
    return str(plan.get("action") or plan.get("action_type") or "maintain")


def _get_target_temp(plan: Dict[str, Any]) -> Optional[float]:
    tt = plan.get("target_temp")
    return None if tt is None else float(tt)


def _find_events(events: List[Dict[str, Any]], unit_id: str) -> List[Dict[str, Any]]:
    return [e for e in events if e.get("unit_id") == unit_id]


def _has_data_quality_block(events_unit: List[Dict[str, Any]]) -> bool:
    """
    Fail-safe: ako ima ozbiljan data_quality -> blokiraj automatiku.
    Ovdje računamo da su validate_readings events data_quality.
    """
    for e in events_unit:
        if e.get("category") == "data_quality" and e.get("severity") in ("high", "critical"):
            return True
        # compatibility: stari validator event types
        if e.get("type") in ("energy_negative", "humidity_out_of_range", "occupancy_invalid") and e.get("severity") in ("high", "critical"):
            return True
    return False


def _get_temp_below_comfort_event(events_unit: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for e in events_unit:
        if e.get("type") == "temp_below_comfort":
            return e
    return None


def _calc_confidence(plan: Dict[str, Any], pred: Dict[str, Any]) -> float:
    """
    Simple confidence:
    - base = prediction confidence (0..1)
    - penalize aggressive actions when predicted consumption is low or savings tiny
    - penalize if occupancy likely high but plan tries to reduce/setback
    """
    base = float(pred.get("confidence") or plan.get("confidence") or 0.5)
    base = max(0.0, min(1.0, base))

    action = _get_plan_action(plan)
    pred_cons = _get_predicted_consumption(pred)
    est_sav = float(plan.get("estimated_savings") or 0.0)
    occ_prob = plan.get("predicted_occupancy_prob", pred.get("predicted_occupancy_prob"))
    occ_prob = None if occ_prob is None else float(occ_prob)

    aggressive = ("reduce_heating" in action) or ("setback" in action)

    penalty = 0.0
    if aggressive:
        if pred_cons < 0.5:
            penalty += 0.15
        if est_sav < 0.02:
            penalty += 0.10
        if occ_prob is not None and occ_prob > OCC_HIGH_TH:
            penalty += 0.10

    conf = base - penalty
    return max(0.0, min(1.0, conf))


def decision_node(state: GraphState) -> GraphState:
    """
    Agent 4: Decision
    Input:
      - optimization_plans (heuristic plans)
      - predictions (ML outputs)
      - anomalies (monitoring events)
    Output:
      - final_decisions (what we actually do)
      - decisions_log insert
    """
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]  # ✅ offline anchor (deterministic)

        plans: Dict[str, Any] = state.get("optimization_plans") or {}
        preds: Dict[str, Any] = state.get("predictions") or {}
        events: List[Dict[str, Any]] = state.get("anomalies") or []

        if not plans:
            state["execution_log"].append(f"Decision(v2): skipped (no plans) anchor={anchor_ts}")
            return state

        final_decisions: List[Dict[str, Any]] = []
        rows_for_db: List[Dict[str, Any]] = []

        approved_cnt = 0
        blocked_cnt = 0
        overridden_cnt = 0

        for unit_id, plan in plans.items():
            pred = preds.get(unit_id, {})

            plan_action = _get_plan_action(plan)
            target_temp = _get_target_temp(plan)
            ts_target = plan.get("timestamp_target") or plan.get("start_time") or pred.get("timestamp_target")

            unit_events = _find_events(events, unit_id)

            # 1) Fail-safe block for data-quality
            if _has_data_quality_block(unit_events):
                approved = 0
                action = "no_action"
                reasoning_notes = ["fail_safe:data_quality_block"]
                confidence = 0.0
                blocked_cnt += 1
            else:
                # 2) Compute confidence
                confidence = _calc_confidence(plan, pred)

                # 3) Start from plan as default decision
                action = plan_action
                approved = 1 if confidence >= CONF_APPROVE_TH else 0
                reasoning_notes = [f"conf={confidence:.2f}", f"plan={plan_action}"]

                # 4) Comfort override: if temp_below_comfort, do NOT allow deep setback
                temp_ev = _get_temp_below_comfort_event(unit_events)
                if temp_ev and target_temp is not None and target_temp < MIN_COMFORT_TEMP:
                    target_temp = MIN_COMFORT_TEMP
                    action = "maintain_min_comfort"
                    approved = 1  # we still approve comfort-safety action
                    overridden_cnt += 1
                    reasoning_notes.append(f"override:temp_below_comfort={temp_ev.get('value')}")

                # 5) Occupancy override: if likely occupied, prefer comfort
                occ_prob = plan.get("predicted_occupancy_prob", pred.get("predicted_occupancy_prob"))
                occ_prob_f = None if occ_prob is None else float(occ_prob)
                if occ_prob_f is not None and occ_prob_f > OCC_HIGH_TH:
                    if target_temp is not None and target_temp < 20.0:
                        target_temp = 20.0
                        action = "maintain_occupied"
                        approved = 1
                        overridden_cnt += 1
                        reasoning_notes.append(f"override:occ_prob_high={occ_prob_f:.2f}")

                # 6) If not approved -> fall back to maintain (safe)
                if approved == 0:
                    action = "maintain"
                    reasoning_notes.append("fallback:not_approved")
                else:
                    approved_cnt += 1

            # Build final decision object
            decision = {
                "timestamp": anchor_ts,
                "building_id": building_id,
                "unit_id": unit_id,
                "timestamp_target": ts_target,
                "action": action,
                "target_temp": target_temp,
                "approved": bool(approved),
                "confidence": float(confidence),
                "mode": "learning",
                "reasoning": {
                    "plan_action": plan_action,
                    "predicted_kwh_interval": _get_predicted_consumption(pred),
                    "predicted_occupancy_prob": plan.get("predicted_occupancy_prob", pred.get("predicted_occupancy_prob")),
                    "estimated_cost": plan.get("estimated_cost"),
                    "estimated_savings": plan.get("estimated_savings"),
                    "notes": reasoning_notes,
                },
            }
            final_decisions.append(decision)

            rows_for_db.append({
                "timestamp": anchor_ts,
                "building_id": building_id,
                "unit_id": unit_id,
                "action": action if target_temp is None else f"{action} target_temp={target_temp}",
                "approved": approved,
                "reasoning_text": "; ".join(reasoning_notes) if reasoning_notes else None,
                "confidence": float(confidence),
                "mode": "learning",
            })

        with connect() as conn:
            insert_decisions_rows(conn, rows_for_db)

        state["final_decisions"] = final_decisions
        state["execution_log"].append(
            f"Decision(v2): anchor={anchor_ts} decisions={len(final_decisions)} "
            f"approved={approved_cnt} blocked={blocked_cnt} overridden={overridden_cnt}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
