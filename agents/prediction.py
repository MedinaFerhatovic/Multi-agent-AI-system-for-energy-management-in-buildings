from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple, List
import numpy as np

from workflow.state_schema import GraphState
from utils.db_helper import (
    connect,
    load_active_consumption_model,
    fetch_recent_series_for_unit,
    insert_predictions_rows,
)

LOOKBACK = 48  # 24h for 30-min data


def _parse_iso(ts: str) -> datetime:
    # supports "2026-01-11T12:30:00Z" and "2026-01-11 12:30:00"
    ts2 = ts.replace("Z", "+00:00").replace(" ", "T")
    return datetime.fromisoformat(ts2)


def _infer_interval(records: List[Dict[str, Any]]) -> timedelta:
    # use last two timestamps if possible, else default 30 min
    if len(records) >= 2:
        t1 = _parse_iso(records[-2]["timestamp"])
        t2 = _parse_iso(records[-1]["timestamp"])
        dt = t2 - t1
        if dt.total_seconds() > 0:
            return dt
    return timedelta(minutes=30)


def _build_features(records: List[Dict[str, Any]]) -> np.ndarray:
    """
    Must match your training features (same as scripts/test_model.py):
    17 features:
      mean/std/max/min/last energy,
      mean/last occupancy,
      mean temp, target temp, wind, cloud,
      area,
      hour,dow,is_weekend,sin,cos
    """
    # we predict "next step", so we use last LOOKBACK as history
    seq = records[-LOOKBACK:]
    target = records[-1]  # last known point, we predict next after it

    e_seq = np.array([r["energy"] for r in seq], dtype=float)
    o_seq = np.array([r["occupancy"] for r in seq], dtype=float)
    t_seq = np.array([r["temp_external"] for r in seq], dtype=float)

    dt = _parse_iso(target["timestamp"])
    hour = dt.hour
    dow = dt.weekday()
    is_weekend = 1 if dow >= 5 else 0

    feats = np.array([
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
    ], dtype=float)

    return feats


def prediction_node(state: GraphState) -> GraphState:
    try:
        building_id = state["building_id"]

        with connect() as conn:
            model_id, model, scaler, model_conf = load_active_consumption_model(conn)
            model_conf = round(float(model_conf), 1)
            predictions: Dict[str, Dict[str, Any]] = {}
            rows_to_insert: List[Dict[str, Any]] = []

            for unit_id in state["validated_data"].keys():
                records = fetch_recent_series_for_unit(conn, unit_id, lookback=LOOKBACK)

                if len(records) < LOOKBACK:
                    # not enough history
                    continue

                interval = _infer_interval(records)
                last_ts = _parse_iso(records[-1]["timestamp"])
                target_ts = (last_ts + interval).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

                x = _build_features(records)
                xs = scaler.transform([x])
                pred = round(float(model.predict(xs)[0]), 1)

                predictions[unit_id] = {
                    "timestamp_target": target_ts,
                    "consumption": pred,
                    "model_id": model_id,
                    "confidence": model_conf,
                }

                rows_to_insert.append({
                    "timestamp_created": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "timestamp_target": target_ts,
                    "building_id": building_id,
                    "unit_id": unit_id,
                    "predicted_consumption": pred,
                    "predicted_occupancy_prob": None,
                    "model_name": model_id,
                    "confidence": model_conf,
                })

            # write to DB
            insert_predictions_rows(conn, rows_to_insert)

        state["predictions"] = predictions
        state["execution_log"].append(f"Prediction: units_predicted={len(predictions)} model={predictions and next(iter(predictions.values()))['model_id']}")
    except Exception as e:
        state["errors"].append(str(e))

    return state
