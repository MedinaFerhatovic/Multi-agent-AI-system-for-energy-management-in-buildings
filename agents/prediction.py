from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import numpy as np

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    load_active_consumption_model,
    fetch_recent_series_for_unit_asof,   
    insert_predictions_rows,
)

LOOKBACK = 48                # 24h for 30-min data
OCC_WINDOW_HOURS = 24        # 24h rolling occupancy probability


def _parse_iso(ts: str) -> datetime:
    ts2 = ts.replace("Z", "+00:00").replace(" ", "T")
    return datetime.fromisoformat(ts2)


def _infer_interval(records: List[Dict[str, Any]]) -> timedelta:
    if len(records) >= 2:
        t1 = _parse_iso(records[-2]["timestamp"])
        t2 = _parse_iso(records[-1]["timestamp"])
        dt = t2 - t1
        if dt.total_seconds() > 0:
            return dt
    return timedelta(minutes=30)


def _build_features(records: List[Dict[str, Any]]) -> np.ndarray:
    seq = records[-LOOKBACK:]
    target = records[-1]

    e_seq = np.array([r["energy"] for r in seq], dtype=float)
    o_seq = np.array([r["occupancy"] for r in seq], dtype=float)
    t_seq = np.array([r["temp_external"] for r in seq], dtype=float)

    dt = _parse_iso(target["timestamp"])
    hour = dt.hour
    dow = dt.weekday()
    is_weekend = 1 if dow >= 5 else 0

    feats = np.array(
        [
            float(np.mean(e_seq)),
            float(np.std(e_seq)),
            float(np.max(e_seq)),
            float(np.min(e_seq)),
            float(e_seq[-1]),

            float(np.mean(o_seq)),
            float(o_seq[-1]),

            float(np.mean(t_seq)),
            float(target["temp_external"]),
            float(target["wind_speed_kmh"]),
            float(target["cloud_cover"]),

            float(target["area_m2"]),

            float(hour),
            float(dow),
            float(is_weekend),
            float(np.sin(2 * np.pi * hour / 24)),
            float(np.cos(2 * np.pi * hour / 24)),
        ],
        dtype=float,
    )
    return feats


def _occupancy_prob_from_recent(records: List[Dict[str, Any]], window_hours: int) -> Optional[float]:
    if not records:
        return None

    interval = _infer_interval(records)
    sec = interval.total_seconds() if interval.total_seconds() > 0 else 1800.0

    points_needed = int(round((window_hours * 3600) / sec))
    points_needed = max(1, points_needed)

    tail = records[-min(len(records), points_needed):]
    occ_vals = [float(r.get("occupancy", 0.0)) for r in tail]
    if not occ_vals:
        return None

    p = float(np.mean(occ_vals))
    p = max(0.0, min(1.0, p))
    return round(p, 3)


def prediction_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]
        anchor_ts = state["timestamp"]  

        with connect() as conn:
            model_id, model, scaler, model_conf = load_active_consumption_model(conn)
            model_conf = round(float(model_conf), 2)

            predictions: Dict[str, Dict[str, Any]] = {}
            rows_to_insert: List[Dict[str, Any]] = []

            for unit_id in state["validated_data"].keys():
                records = fetch_recent_series_for_unit_asof(
                    conn, unit_id=unit_id, anchor_ts=anchor_ts, lookback=LOOKBACK
                )

                if len(records) < LOOKBACK:
                    continue

                interval = _infer_interval(records)
                last_ts = _parse_iso(records[-1]["timestamp"])
                target_ts = (last_ts + interval).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

                x = _build_features(records)
                xs = scaler.transform([x])
                pred_kwh = round(float(model.predict(xs)[0]), 3)

                occ_prob = _occupancy_prob_from_recent(records, window_hours=OCC_WINDOW_HOURS)

                predictions[unit_id] = {
                    "timestamp_target": target_ts,
                    "predicted_consumption": pred_kwh,
                    "predicted_occupancy_prob": occ_prob,
                    "model_id": model_id,
                    "confidence": model_conf,
                }

                rows_to_insert.append(
                    {
                        "timestamp_created": anchor_ts,
                        "timestamp_target": target_ts,
                        "building_id": building_id,
                        "unit_id": unit_id,
                        "predicted_consumption": pred_kwh,
                        "predicted_occupancy_prob": occ_prob,
                        "model_name": model_id,
                        "confidence": model_conf,
                    }
                )

            insert_predictions_rows(conn, rows_to_insert)

        state["predictions"] = predictions
        state["execution_log"].append(
            f"Prediction(off): anchor={anchor_ts} units_predicted={len(predictions)} model={model_id} occ_window_h={OCC_WINDOW_HOURS}"
        )

    except Exception as e:
        state["errors"].append(str(e))

    return state
