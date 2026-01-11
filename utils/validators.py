def validate_readings(raw):
    validated = {}
    anomalies = []

    for unit_id, sensors in raw.items():
        for stype, r in sensors.items():
            ts = r["timestamp"]
            v = r["value"]

            if stype == "energy" and v < 0:
                anomalies.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "energy_negative",
                    "value": v,
                    "severity": "high",
                    "action": "drop"
                })
                continue

            if stype == "temp_internal" and not (18 <= v <= 28):
                anomalies.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "temp_out_of_range",
                    "value": v,
                    "severity": "high",
                    "action": "drop"
                })
                continue

            if stype == "humidity" and not (0 <= v <= 100):
                anomalies.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "humidity_out_of_range",
                    "value": v,
                    "severity": "medium",
                    "action": "drop"
                })
                continue

            if stype == "occupancy" and v not in (0, 1):
                anomalies.append({
                    "timestamp": ts,
                    "unit_id": unit_id,
                    "type": "occupancy_invalid",
                    "value": v,
                    "severity": "low",
                    "action": "drop"
                })
                continue

            validated.setdefault(unit_id, {})
            validated[unit_id][stype] = r

    return validated, anomalies
