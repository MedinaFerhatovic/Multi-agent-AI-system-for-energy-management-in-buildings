# agents/data_monitor.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    get_latest_readings_asof,
    get_recent_readings,
    get_sensor_id,
    get_tariff_model,
    insert_anomalies,
)
from utils.validators import validate_readings


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_hour_min(ts: str) -> Tuple[int, int]:
    if "T" in ts:
        hh = int(ts[11:13])
        mm = int(ts[14:16])
    else:
        hh = int(ts[11:13])
        mm = int(ts[14:16])
    return hh, mm


def _is_sunday(ts: str) -> bool:
    y = int(ts[0:4]); m = int(ts[5:7]); d = int(ts[8:10])
    import datetime as _dt
    return _dt.date(y, m, d).weekday() == 6


def _is_low_tariff(ts: str, tariff: Dict[str, Any]) -> bool:
    if tariff.get("sunday_all_day_low", 0) == 1 and _is_sunday(ts):
        return True

    low_start = tariff.get("low_tariff_start", "22:00")
    low_end = tariff.get("low_tariff_end", "06:00")

    sh = int(low_start[:2]); sm = int(low_start[3:5])
    eh = int(low_end[:2]); em = int(low_end[3:5])

    hh, mm = _parse_hour_min(ts)
    cur = hh * 60 + mm
    start = sh * 60 + sm
    end = eh * 60 + em

    if start > end:
        return cur >= start or cur < end
    return start <= cur < end


def _avg(vals: List[float]) -> Optional[float]:
    return sum(vals) / len(vals) if vals else None


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
    latest: Optional[Dict[str, Any]],
    tariff: Dict[str, Any],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not latest or "timestamp" not in latest or "value" not in latest:
        return events

    ts_latest = latest["timestamp"]
    v_latest = float(latest["value"])

    energy_vals = [v for _, v in series_energy] if series_energy else []
    e_avg = _avg(energy_vals)
    e_std = _std(energy_vals)

    if e_avg is not None and e_avg > 0:
        spike = False
        if e_std is not None:
            if v_latest > (e_avg + 3.0 * e_std):
                spike = True
        else:
            if v_latest > 2.5 * e_avg:
                spike = True

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
                }
            })

    unocc_energy: List[float] = []
    if series_occ and series_energy:
        occ_map = {ts: v for ts, v in series_occ}
        for ts, ev in series_energy:
            if occ_map.get(ts, 1.0) == 0.0:
                unocc_energy.append(ev)

    unocc_avg = _avg(unocc_energy)
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
                "details": {"unocc_avg_kwh": round(unocc_avg, 4)}
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
                "details": {"unocc_avg_kwh": None}
            })

    low = _is_low_tariff(ts_latest, tariff)
    price = float(tariff["low_price_per_kwh"] if low else tariff["high_price_per_kwh"])
    currency = tariff.get("currency", "BAM")
    est_cost = v_latest * price

    if (not low) and e_avg is not None and v_latest > max(0.35, 1.5 * e_avg):
        events.append({
            "timestamp": ts_latest,
            "unit_id": unit_id,
            "type": "high_cost_now",
            "value": est_cost,
            "severity": "medium",
            "action": "alert",
            "category": "operational",
            "details": {
                "kwh_interval": round(v_latest, 4),
                "price_per_kwh": price,
                "currency": currency,
                "low_tariff": low,
            }
        })

    return events


def data_monitor_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]

        with connect() as conn:
            anchor_ts = state["timestamp"]
            anchor_dt = datetime.fromisoformat(anchor_ts.replace("Z", "+00:00"))
            start_dt = anchor_dt - timedelta(hours=24)
            state["execution_log"].append(
               f"Recent window: {start_dt.isoformat().replace('+00:00','Z')} .. {anchor_ts}"
            )
            raw_latest = get_latest_readings_asof(conn, building_id, anchor_ts)

            # ✅ DEBUG/ASSERT ENERGY (LATEST)
            units_with_energy = sum(1 for _, s in raw_latest.items() if "energy" in s)
            state["execution_log"].append(
                f"Energy(latest): units_with_energy={units_with_energy}/{len(raw_latest)} anchor_ts={anchor_ts}"
            )

            validated, base_events = validate_readings(raw_latest)

            recent = get_recent_readings(
                conn,
                building_id,
                sensor_types=["energy", "occupancy", "temp_internal"],
                lookback_hours=24,
                anchor_ts=anchor_ts,
            )

            # ✅ DEBUG/ASSERT ENERGY (RECENT WINDOW)
            recent_energy_points = sum(len(recent.get(u, {}).get("energy", [])) for u in recent)
            recent_occ_points = sum(len(recent.get(u, {}).get("occupancy", [])) for u in recent)
            state["execution_log"].append(
                f"Energy(recent): energy_points={recent_energy_points} occ_points={recent_occ_points} lookback_h=24"
            )

            tariff = get_tariff_model(conn, building_id)

            energy_events: List[Dict[str, Any]] = []
            for unit_id, latest_sensors in raw_latest.items():
                latest_energy = latest_sensors.get("energy")
                series_energy = recent.get(unit_id, {}).get("energy", [])
                series_occ = recent.get(unit_id, {}).get("occupancy", [])

                energy_events.extend(
                    _energy_events_for_unit(
                        unit_id=unit_id,
                        series_energy=series_energy,
                        series_occ=series_occ,
                        latest=latest_energy,
                        tariff=tariff,
                    )
                )

            all_events = base_events + energy_events
            for ev in all_events:
                ev["building_id"] = building_id

                st_guess = None
                t = ev.get("type", "")
                if t.startswith("energy") or t in ("high_energy_unoccupied", "high_cost_now"):
                    st_guess = "energy"
                elif t.startswith("temp") or "temp" in t:
                    st_guess = "temp_internal"
                elif t.startswith("occupancy"):
                    st_guess = "occupancy"
                elif "humidity" in t:
                    st_guess = "humidity"

                if st_guess:
                    ev["sensor_id"] = get_sensor_id(conn, ev["unit_id"], st_guess)

            insert_anomalies(conn, all_events)

        state["sensor_data"] = raw_latest
        state["validated_data"] = validated
        state["anomalies"] = all_events

        state["monitoring_summary"] = {
            "tariff_currency": tariff.get("currency", "BAM"),
            "low_tariff_start": tariff.get("low_tariff_start"),
            "low_tariff_end": tariff.get("low_tariff_end"),
        }

        state["execution_log"].append(
            f"DataMonitor(v2): units={len(raw_latest)} events={len(all_events)} validated_units={len(validated)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
