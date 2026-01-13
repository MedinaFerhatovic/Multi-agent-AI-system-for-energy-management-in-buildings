# agents/decision.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from datetime import timezone

from workflow.state_schema import GraphState
from utils.db_helper import connect, insert_decisions_rows, insert_validation_log


# thresholds (možeš fino naštimati)
CONF_APPROVE_TH = 0.60
OCC_HIGH_TH = 0.60
MIN_COMFORT_TEMP = 19.0
MIN_CONFIDENCE = 0.45
MIN_COVERAGE = 0.60
MAX_PRED_KWH = 10.0
GLOBAL_BLOCK_RATIO = 0.40

# 🆕 Energy alert thresholds
ENERGY_SPIKE_EMERGENCY_TEMP = 18.0
SUSTAINED_HIGH_TEMP_REDUCTION = 1.0  # °C smanjenje
BUDGET_EXCEEDED_WARNING_ONLY = True  # samo loguj, ne mijenjaj akciju


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


# 🆕 Helper functions for energy alerts
def _get_energy_spike_event(events_unit: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pronađi energy_spike event"""
    for e in events_unit:
        if e.get("type") == "energy_spike" and e.get("category") == "operational":
            return e
    return None


def _get_sustained_high_event(events_unit: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pronađi sustained_high_consumption event"""
    for e in events_unit:
        if e.get("type") == "sustained_high_consumption" and e.get("category") == "operational":
            return e
    return None


def _get_energy_waste_event(events_unit: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pronađi energy_waste_rising event"""
    for e in events_unit:
        if e.get("type") == "energy_waste_rising" and e.get("category") == "operational":
            return e
    return None


def _get_budget_exceeded_event(events_unit: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pronađi daily_budget_exceeded event"""
    for e in events_unit:
        if e.get("type") == "daily_budget_exceeded" and e.get("category") == "operational":
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


def _validate_run(state: GraphState) -> Dict[str, Any]:
    preds: Dict[str, Any] = state.get("predictions") or {}
    validated = state.get("validated_data") or {}
    events: List[Dict[str, Any]] = state.get("anomalies") or []

    unit_count = max(len(validated), len(preds))
    coverage = (len(preds) / unit_count) if unit_count else 0.0

    conf_vals = []
    invalid_units = []
    for unit_id, pred in preds.items():
        conf = pred.get("confidence")
        if conf is not None:
            conf_vals.append(float(conf))
        pred_cons = pred.get("predicted_consumption", pred.get("consumption"))
        if pred_cons is not None:
            pc = float(pred_cons)
            if pc < 0.0 or pc > MAX_PRED_KWH:
                invalid_units.append(unit_id)

    conf_avg = round(sum(conf_vals) / len(conf_vals), 3) if conf_vals else 0.0

    block_units = set()
    for e in events:
        if e.get("category") == "data_quality" and e.get("severity") in ("high", "critical"):
            if e.get("unit_id"):
                block_units.add(e["unit_id"])

    reasons = []
    status = "ok"
    global_block = False

    if coverage < MIN_COVERAGE:
        status = "degraded"
        reasons.append(f"low_coverage:{coverage:.2f}")

    if conf_avg < MIN_CONFIDENCE:
        status = "degraded"
        reasons.append(f"low_confidence:{conf_avg:.2f}")

    if invalid_units:
        status = "degraded"
        reasons.append(f"invalid_predictions:{len(invalid_units)}")

    bad_ratio = 0.0
    if unit_count:
        bad_ratio = len(set(invalid_units) | block_units) / float(unit_count)

    if bad_ratio >= GLOBAL_BLOCK_RATIO:
        status = "blocked"
        global_block = True
        reasons.append(f"global_block_ratio:{bad_ratio:.2f}")

    return {
        "status": status,
        "global_block": global_block,
        "model_confidence_avg": conf_avg,
        "coverage": round(coverage, 3),
        "block_units": sorted(block_units),
        "invalid_units": sorted(set(invalid_units)),
        "reasons": reasons,
    }


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
        report = _validate_run(state)
        state["validation_report"] = report
        global_block = bool(report.get("global_block", False))
        block_units = set(report.get("block_units", []))

        if not plans:
            state["execution_log"].append(f"Decision(v2): skipped (no plans) anchor={anchor_ts}")
            return state

        final_decisions: List[Dict[str, Any]] = []
        rows_for_db: List[Dict[str, Any]] = []

        approved_cnt = 0
        blocked_cnt = 0
        overridden_cnt = 0
        energy_alert_overrides = 0  # 🆕 Counter za energy alert overrides

        approve_th = CONF_APPROVE_TH if report.get("status") != "degraded" else max(CONF_APPROVE_TH, 0.75)

        for unit_id, plan in plans.items():
            pred = preds.get(unit_id, {})

            plan_action = _get_plan_action(plan)
            target_temp = _get_target_temp(plan)
            ts_target = plan.get("timestamp_target") or plan.get("start_time") or pred.get("timestamp_target")

            unit_events = _find_events(events, unit_id)

            # 1) Validation gate or fail-safe block for data-quality
            if global_block or unit_id in block_units:
                approved = 0
                action = "no_action"
                reasoning_notes = ["validation_block"]
                confidence = 0.0
                blocked_cnt += 1
            elif _has_data_quality_block(unit_events):
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
                approved = 1 if confidence >= approve_th else 0
                reasoning_notes = [f"conf={confidence:.2f}", f"plan={plan_action}"]

                # =====================================================
                # 🆕 4) ENERGY ALERT OVERRIDES (highest priority)
                # =====================================================
                
                # 4a) ENERGY SPIKE - Emergency action
                spike_event = _get_energy_spike_event(unit_events)
                if spike_event:
                    spike_value = spike_event.get("value", 0)
                    action = "emergency_reduce_heating"
                    target_temp = ENERGY_SPIKE_EMERGENCY_TEMP
                    approved = 1  # Force approve emergency action
                    confidence = 0.95  # High confidence za emergency
                    overridden_cnt += 1
                    energy_alert_overrides += 1
                    reasoning_notes.append(f"🚨EMERGENCY:energy_spike={spike_value}kWh")
                    reasoning_notes.append(f"forced_temp={ENERGY_SPIKE_EMERGENCY_TEMP}°C")
                
                # 4b) SUSTAINED HIGH CONSUMPTION - Gradual reduction
                elif not spike_event:  # Samo ako nema spike-a (spike je prioritetniji)
                    sustained_event = _get_sustained_high_event(unit_events)
                    if sustained_event:
                        sustained_value = sustained_event.get("value", 0)
                        details = sustained_event.get("details", {})
                        percent_increase = details.get("percent_increase", 0)
                        
                        # Ako nije već na emergency akciji, smanji temperaturu
                        if target_temp and target_temp > (MIN_COMFORT_TEMP + 0.5):
                            target_temp -= SUSTAINED_HIGH_TEMP_REDUCTION
                            target_temp = max(target_temp, MIN_COMFORT_TEMP)  # Ne idi ispod minimuma
                            action = "reduce_heating_sustained_high"
                            approved = 1
                            overridden_cnt += 1
                            energy_alert_overrides += 1
                            reasoning_notes.append(f"⚠️sustained_high:24h_avg={sustained_value}kWh")
                            reasoning_notes.append(f"increase={percent_increase}%")
                            reasoning_notes.append(f"temp_reduced_by={SUSTAINED_HIGH_TEMP_REDUCTION}°C")
                
                # 4c) ENERGY WASTE RISING - Aggressive reduction
                waste_event = _get_energy_waste_event(unit_events)
                if waste_event and not spike_event:  # Ako nema spike-a
                    waste_value = waste_event.get("value", 0)
                    
                    # Ako potrošnja raste bez opravdanja, primijeni štednju
                    if "setback" not in action and "reduce" not in action:
                        action = "reduce_heating_waste_detected"
                        if target_temp and target_temp > MIN_COMFORT_TEMP:
                            target_temp = max(MIN_COMFORT_TEMP, target_temp - 0.5)
                        approved = 1
                        overridden_cnt += 1
                        energy_alert_overrides += 1
                        reasoning_notes.append(f"⚠️energy_waste:rising_trend={waste_value}kWh")
                
                # 4d) DAILY BUDGET EXCEEDED - Warning only (ne mijenjaj akciju)
                budget_event = _get_budget_exceeded_event(unit_events)
                if budget_event:
                    details = budget_event.get("details", {})
                    daily_kwh = details.get("daily_consumption_kwh", 0)
                    overage = details.get("overage_kwh", 0)
                    cost = details.get("cost_estimate", 0)
                    
                    if BUDGET_EXCEEDED_WARNING_ONLY:
                        # Samo loguj, ne mijenjaj akciju
                        reasoning_notes.append(f"💰budget_exceeded:daily={daily_kwh}kWh")
                        reasoning_notes.append(f"overage={overage}kWh cost={cost}BAM")
                    else:
                        # Opciono: Agresivnija akcija ako prelazi budžet
                        if overage > 2.0:  # Ako je overage značajan
                            action = "reduce_heating_budget"
                            if target_temp and target_temp > MIN_COMFORT_TEMP:
                                target_temp = MIN_COMFORT_TEMP
                            approved = 1
                            overridden_cnt += 1
                            energy_alert_overrides += 1
                            reasoning_notes.append(f"💰BUDGET:overage={overage}kWh forced_reduction")

                # =====================================================
                # 5) Comfort override (ako nema energy emergency)
                # =====================================================
                if not spike_event:  # Comfort override samo ako nema emergency
                    temp_ev = _get_temp_below_comfort_event(unit_events)
                    if temp_ev and target_temp is not None and target_temp < MIN_COMFORT_TEMP:
                        target_temp = MIN_COMFORT_TEMP
                        action = "maintain_min_comfort"
                        approved = 1
                        overridden_cnt += 1
                        reasoning_notes.append(f"override:temp_below_comfort={temp_ev.get('value')}")

                # 6) Occupancy override (samo ako nema energy emergency)
                if not spike_event:
                    occ_prob = plan.get("predicted_occupancy_prob", pred.get("predicted_occupancy_prob"))
                    occ_prob_f = None if occ_prob is None else float(occ_prob)
                    if occ_prob_f is not None and occ_prob_f > OCC_HIGH_TH:
                        if target_temp is not None and target_temp < 20.0:
                            target_temp = 20.0
                            action = "maintain_occupied"
                            approved = 1
                            overridden_cnt += 1
                            reasoning_notes.append(f"override:occ_prob_high={occ_prob_f:.2f}")

                # 7) If not approved -> fall back to maintain (safe)
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
                "action": action if target_temp is None else f"{action} target_temp={target_temp}°C",
                "approved": approved,
                "reasoning_text": "; ".join(reasoning_notes) if reasoning_notes else None,
                "confidence": float(confidence),
                "mode": "learning",
            })

        with connect() as conn:
            insert_decisions_rows(conn, rows_for_db)
            insert_validation_log(
                conn,
                {
                    "timestamp": anchor_ts,
                    "building_id": building_id,
                    "status": report["status"],
                    "model_confidence_avg": report["model_confidence_avg"],
                    "coverage": report["coverage"],
                    "blocked_units_count": len(report.get("block_units", [])),
                    "invalid_units_count": len(report.get("invalid_units", [])),
                    "reasons_json": ";".join(report.get("reasons", [])),
                },
            )

        state["final_decisions"] = final_decisions
        state["execution_log"].append(
            f"Decision(v2): anchor={anchor_ts} decisions={len(final_decisions)} "
            f"approved={approved_cnt} blocked={blocked_cnt} overridden={overridden_cnt} "
            f"energy_overrides={energy_alert_overrides}"  # 🆕 Log energy overrides
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state