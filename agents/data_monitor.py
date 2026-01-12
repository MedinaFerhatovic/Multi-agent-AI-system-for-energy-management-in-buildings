# agents/data_monitor.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    get_latest_readings_asof,
    get_recent_readings,
    get_sensor_id,
    get_tariff_for_building,
    insert_anomalies,
)
from utils.validators import validate_readings


# -----------------------
# small helpers
# -----------------------
def _parse_hour_min(ts: str) -> Tuple[int, int]:
    # works with "YYYY-MM-DDTHH:MM:SSZ" and "YYYY-MM-DD HH:MM:SS"
    if "T" in ts:
        hh = int(ts[11:13])
        mm = int(ts[14:16])
    else:
        hh = int(ts[11:13])
        mm = int(ts[14:16])
    return hh, mm


def _is_sunday(ts: str) -> bool:
    y = int(ts[0:4])
    m = int(ts[5:7])
    d = int(ts[8:10])
    import datetime as _dt
    return _dt.date(y, m, d).weekday() == 6  # Sunday


def _is_low_tariff(ts: str, tariff: Dict[str, Any]) -> bool:
    if int(tariff.get("sunday_all_day_low", 0)) == 1 and _is_sunday(ts):
        return True

    low_start = tariff.get("low_tariff_start", "22:00")
    low_end = tariff.get("low_tariff_end", "06:00")

    sh, sm = int(low_start[:2]), int(low_start[3:5])
    eh, em = int(low_end[:2]), int(low_end[3:5])

    hh, mm = _parse_hour_min(ts)
    cur = hh * 60 + mm
    start = sh * 60 + sm
    end = eh * 60 + em

    # wrap window (22:00 -> 06:00)
    if start > end:
        return (cur >= start) or (cur < end)
    return start <= cur < end


def _avg(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _std(vals: List[float]) -> Optional[float]:
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / len(vals)
    return var ** 0.5


def _energy_events_for_unit(
    unit_id: str,
    series_energy: List[Tuple[str, float]],
    series_occ: List[Tuple[str, float]],
    latest_energy: Optional[Dict[str, Any]],
    tariff: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Energy-focused operational events:
      - energy_spike (relative to last 24h)
      - high_energy_unoccupied
      - high_cost_now (high tariff + above typical)
    """
    events: List[Dict[str, Any]] = []
    if not latest_energy or "timestamp" not in latest_energy or "value" not in latest_energy:
        return events

    ts_latest = latest_energy["timestamp"]
    v_latest = float(latest_energy["value"])

    e_vals = [v for _, v in series_energy] if series_energy else []
    e_avg = _avg(e_vals)
    e_std = _std(e_vals)

    # 1) spike
    if e_avg is not None and e_avg > 0:
        spike = False
        if e_std is not None:
            spike = v_latest > (e_avg + 3.0 * e_std)
        else:
            spike = v_latest > 2.5 * e_avg

        if spike:
            events.append({
                "timestamp": ts_latest,
                "unit_id": unit_id,
                "type": "energy_spike",
                "value": v_latest,
                "severity": "high",
                "action": "investigate",
                "category": "operational",
                "details": {
                    "avg_kwh": round(e_avg, 4),
                    "std_kwh": None if e_std is None else round(e_std, 4),
                },
            })

    # 2) high energy while unoccupied
    # map occupancy by timestamp (sim uses same timestamp across sensors)
    occ_map = {ts: float(v) for ts, v in (series_occ or [])}
    unocc_vals: List[float] = []
    for ts, ev in (series_energy or []):
        if occ_map.get(ts, 1.0) == 0.0:
            unocc_vals.append(float(ev))
    unocc_avg = _avg(unocc_vals)

    latest_occ = None
    if series_occ:
        latest_occ = float(series_occ[-1][1])

    if latest_occ == 0.0:
        if unocc_avg is not None and unocc_avg > 0 and v_latest > max(0.35, 2.0 * unocc_avg):
            events.append({
                "timestamp": ts_latest,
                "unit_id": unit_id,
                "type": "high_energy_unoccupied",
                "value": v_latest,
                "severity": "high",
                "action": "alert",
                "category": "operational",
                "details": {"unocc_avg_kwh": round(unocc_avg, 4)},
            })
        elif unocc_avg is None and v_latest > 0.6:
            events.append({
                "timestamp": ts_latest,
                "unit_id": unit_id,
                "type": "high_energy_unoccupied",
                "value": v_latest,
                "severity": "medium",
                "action": "investigate",
                "category": "operational",
                "details": {"unocc_avg_kwh": None},
            })

    # 3) high cost now (not necessarily anomaly, but useful signal)
    low = _is_low_tariff(ts_latest, tariff)
    price = float(tariff["low_price_per_kwh"] if low else tariff["high_price_per_kwh"])
    est_cost = v_latest * price

    if (not low) and (e_avg is not None) and v_latest > max(0.35, 1.5 * e_avg):
        events.append({
            "timestamp": ts_latest,
            "unit_id": unit_id,
            "type": "high_cost_now",
            "value": round(est_cost, 4),
            "severity": "medium",
            "action": "alert",
            "category": "operational",
            "details": {
                "kwh_interval": round(v_latest, 4),
                "price_per_kwh": price,
                "currency": tariff.get("currency", "BAM"),
                "low_tariff": low,
            },
        })

    return events


# -----------------------
# main node
# -----------------------
def data_monitor_node(state: GraphState) -> GraphState:
    """
    Offline monitoring:
      - anchor_ts = state['timestamp'] (MAX timestamp iz DB u runneru)
      - latest readings asof anchor
      - validate_readings -> (validated, base_events)
      - recent 24h window -> energy events
      - insert all events into anomalies_log
      - fill state: sensor_data, validated_data, anomalies
    """
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]

        with connect() as conn:
            raw_latest = get_latest_readings_asof(conn, building_id, anchor_ts)

            # IMPORTANT: validate_readings must return (validated, events)
            validated, base_events = validate_readings(raw_latest)

            recent = get_recent_readings(
                conn,
                building_id,
                sensor_types=["energy", "occupancy", "temp_internal", "humidity"],
                lookback_hours=24,
                anchor_ts=anchor_ts,
            )

            # debug counters
            units_with_energy = sum(1 for u in raw_latest if "energy" in raw_latest[u])
            state["execution_log"].append(
                f"Energy(latest): units_with_energy={units_with_energy}/{len(raw_latest)} anchor_ts={anchor_ts}"
            )
            recent_energy_points = sum(len(recent.get(u, {}).get("energy", [])) for u in recent)
            recent_occ_points = sum(len(recent.get(u, {}).get("occupancy", [])) for u in recent)
            state["execution_log"].append(
                f"Energy(recent): energy_points={recent_energy_points} occ_points={recent_occ_points} lookback_h=24"
            )

            tariff = get_tariff_for_building(conn, building_id)

            # build energy events per unit
            energy_events: List[Dict[str, Any]] = []
            for unit_id, sensors in raw_latest.items():
                latest_energy = sensors.get("energy")
                series_energy = recent.get(unit_id, {}).get("energy", [])
                series_occ = recent.get(unit_id, {}).get("occupancy", [])
                energy_events.extend(
                    _energy_events_for_unit(
                        unit_id=unit_id,
                        series_energy=series_energy,
                        series_occ=series_occ,
                        latest_energy=latest_energy,
                        tariff=tariff,
                    )
                )

            # merge + attach building_id + sensor_id
            all_events = (base_events or []) + (energy_events or [])
            for ev in all_events:
                ev["building_id"] = building_id

                st_guess = None
                t = ev.get("type", "")
                if t.startswith("energy") or t in ("high_energy_unoccupied", "high_cost_now"):
                    st_guess = "energy"
                elif "temp" in t:
                    st_guess = "temp_internal"
                elif "occupancy" in t:
                    st_guess = "occupancy"
                elif "humidity" in t:
                    st_guess = "humidity"

                if st_guess:
                    ev["sensor_id"] = get_sensor_id(conn, ev["unit_id"], st_guess)

            insert_anomalies(conn, all_events)

        state["sensor_data"] = raw_latest
        state["validated_data"] = validated
        state["anomalies"] = all_events

        state["execution_log"].append(
            f"DataMonitor(off): building={building_id} anchor={anchor_ts} units={len(raw_latest)} events={len(all_events)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
