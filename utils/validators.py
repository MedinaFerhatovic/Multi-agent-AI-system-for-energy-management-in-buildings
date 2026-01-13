from typing import Dict, Any, List, Tuple, Optional

# Thresholds
ENERGY_MIN = -0.01  # Allow tiny negative for sensor noise
ENERGY_MAX = 15.0   # kWh per 30min interval (30 kW peak)
TEMP_MIN = 5.0      # °C
TEMP_MAX = 35.0     # °C
HUMIDITY_MIN = 10.0 # %
HUMIDITY_MAX = 95.0 # %
OCC_VALID = {0.0, 1.0}  # Binary occupancy


def validate_reading(
    sensor_type: str, 
    value: Optional[float],
    unit_id: str,
    timestamp: str
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Validate single reading. Returns (is_valid, event_or_none)
    """
    if value is None:
        return False, {
            "timestamp": timestamp,
            "unit_id": unit_id,
            "type": f"{sensor_type}_missing",
            "value": None,
            "severity": "high",
            "action": "investigate",
            "category": "data_quality",
            "details": {"reason": "null_value"},
        }
    
    v = float(value)
    
    if sensor_type == "energy":
        if v < ENERGY_MIN:
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "energy_negative",
                "value": v,
                "severity": "critical",
                "action": "investigate",
                "category": "data_quality",
                "details": {"threshold_min": ENERGY_MIN},
            }
        if v > ENERGY_MAX:
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "energy_excessive",
                "value": v,
                "severity": "high",
                "action": "investigate",
                "category": "data_quality",
                "details": {
                    "threshold_max": ENERGY_MAX,
                    "possible_cause": "sensor_malfunction_or_real_spike",
                },
            }
    
    elif sensor_type == "temp_internal":
        if not (TEMP_MIN <= v <= TEMP_MAX):
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "temp_out_of_range",
                "value": v,
                "severity": "high",
                "action": "investigate",
                "category": "data_quality",
                "details": {
                    "min": TEMP_MIN,
                    "max": TEMP_MAX,
                },
            }
        # COMFORT CHECK (not a validation error, but operational alert)
        if v < 18.0:
            return True, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "temp_below_comfort",
                "value": v,
                "severity": "medium",
                "action": "alert",
                "category": "operational",
                "details": {"comfort_threshold": 18.0},
            }
    
    elif sensor_type == "humidity":
        if not (HUMIDITY_MIN <= v <= HUMIDITY_MAX):
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "humidity_out_of_range",
                "value": v,
                "severity": "medium",
                "action": "investigate",
                "category": "data_quality",
                "details": {
                    "min": HUMIDITY_MIN,
                    "max": HUMIDITY_MAX,
                },
            }
    
    elif sensor_type == "occupancy":
        if v not in OCC_VALID:
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "occupancy_invalid",
                "value": v,
                "severity": "medium",
                "action": "investigate",
                "category": "data_quality",
                "details": {
                    "expected": list(OCC_VALID),
                    "received": v,
                },
            }
    
    return True, None


def validate_readings(
    raw_latest: Dict[str, Dict[str, Dict[str, Any]]]
) -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Validate all latest readings.
    
    Returns:
        (validated_data, events)
    
    validated_data: only valid readings
    events: list of validation events (errors + warnings)
    """
    validated = {}
    events = []
    
    for unit_id, sensors in raw_latest.items():
        validated[unit_id] = {}
        
        for sensor_type, reading in sensors.items():
            if not reading or "value" not in reading:
                events.append({
                    "timestamp": reading.get("timestamp") if reading else None,
                    "unit_id": unit_id,
                    "type": f"{sensor_type}_missing_structure",
                    "value": None,
                    "severity": "high",
                    "action": "investigate",
                    "category": "data_quality",
                })
                continue
            
            is_valid, event = validate_reading(
                sensor_type=sensor_type,
                value=reading.get("value"),
                unit_id=unit_id,
                timestamp=reading.get("timestamp"),
            )
            
            if event:
                events.append(event)
            
            if is_valid:
                validated[unit_id][sensor_type] = reading
    
    return validated, events


def validate_prediction(
    unit_id: str,
    pred_kwh: Optional[float],
    occ_prob: Optional[float],
    timestamp: str
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Validate ML prediction output.
    """
    if pred_kwh is None:
        return False, {
            "timestamp": timestamp,
            "unit_id": unit_id,
            "type": "prediction_missing",
            "value": None,
            "severity": "high",
            "action": "investigate",
            "category": "data_quality",
        }
    
    pk = float(pred_kwh)
    
    if pk < 0.0:
        return False, {
            "timestamp": timestamp,
            "unit_id": unit_id,
            "type": "prediction_negative",
            "value": pk,
            "severity": "critical",
            "action": "block_automation",
            "category": "data_quality",
        }
    
    if pk > 10.0:  
        return False, {
            "timestamp": timestamp,
            "unit_id": unit_id,
            "type": "prediction_excessive",
            "value": pk,
            "severity": "high",
            "action": "investigate",
            "category": "data_quality",
            "details": {"threshold_max": 10.0},
        }
    
    if occ_prob is not None:
        op = float(occ_prob)
        if not (0.0 <= op <= 1.0):
            return False, {
                "timestamp": timestamp,
                "unit_id": unit_id,
                "type": "occupancy_prob_invalid",
                "value": op,
                "severity": "medium",
                "action": "investigate",
                "category": "data_quality",
                "details": {"expected_range": [0.0, 1.0]},
            }
    
    return True, None