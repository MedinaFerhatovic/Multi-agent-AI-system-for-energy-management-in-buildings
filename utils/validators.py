# utils/validators.py
from typing import Dict, Any, Tuple, List

def validate_readings(raw: Dict[str, Dict[str, Dict[str, Any]]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns (validated, events)

    events: list of dicts with:
      timestamp, unit_id, type, value, severity, action, category
      category in {"data_quality", "operational"}
    """
    validated: Dict[str, Any] = {}
    events: List[Dict[str, Any]] = []

    for unit_id, sensors in (raw or {}).items():
        for stype, r in (sensors or {}).items():
            ts = r.get("timestamp")
            v = r.get("value")

            # guard
            if ts is None or v is None:
                events.append({
                    "timestamp": ts or "",
                    "unit_id": unit_id,
                    "type": "missing_value",
                    "value": None,
                    "severity": "medium",
                    "action": "drop",
                    "category": "data_quality",
                })
                continue

            # ---- DATA QUALITY (DROP) ----
            if stype == "energy" and v < 0:
                events.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "energy_negative",
                    "value": v,
                    "severity": "high",
                    "action": "drop",
                    "category": "data_quality",
                })
                continue

            if stype == "humidity" and not (0 <= v <= 100):
                events.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "humidity_out_of_range",
                    "value": v,
                    "severity": "high",
                    "action": "drop",
                    "category": "data_quality",
                })
                continue

            if stype == "occupancy" and v not in (0, 1):
                events.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "occupancy_invalid",
                    "value": v,
                    "severity": "high",
                    "action": "drop",
                    "category": "data_quality",
                })
                continue

            # temp_internal: ekstremi su data_quality; normalni out-of-comfort je operational
            if stype == "temp_internal":
                if v < -10 or v > 60:  # fizički/senzorski besmisao
                    events.append({
                        "timestamp": ts,
                        "unit_id": unit_id,
                        "type": "temp_sensor_fault_extreme",
                        "value": v,
                        "severity": "high",
                        "action": "drop",
                        "category": "data_quality",
                    })
                    continue

                # ---- OPERATIONAL (KEEP + ALERT) ----
                if v < 18:
                    events.append({
                        "timestamp": ts,
                        "unit_id": unit_id,
                        "type": "temp_below_comfort",
                        "value": v,
                        "severity": "high",
                        "action": "alert",
                        "category": "operational",
                    })
                elif v > 28:
                    events.append({
                        "timestamp": ts,
                        "unit_id": unit_id,
                        "type": "temp_above_comfort",
                        "value": v,
                        "severity": "high",
                        "action": "alert",
                        "category": "operational",
                    })

            # if we reached here => keep reading
            validated.setdefault(unit_id, {})
            validated[unit_id][stype] = r

    return validated, events
