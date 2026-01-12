# agents/data_monitor.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone

from workflow.state_schema import GraphState
from utils.db_helper import connect, get_latest_readings_asof, insert_anomalies
from utils.validators import validate_readings

def data_monitor_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]  # ✅ anchor iz state-a (offline)

        with connect() as conn:
            raw = get_latest_readings_asof(conn, building_id, anchor_ts)
            validated, anomalies = validate_readings(raw)

            for a in anomalies:
                a["building_id"] = building_id

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
            f"DataMonitor(off): building={building_id} anchor={anchor_ts} units={len(raw)} anomalies={len(anomalies)}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
