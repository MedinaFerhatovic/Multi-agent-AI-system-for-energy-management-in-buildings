import sqlite3
import json
import pickle
from pathlib import Path
from typing import Dict, Any, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# =========================================================
# Agent 1 helpers
# =========================================================
def get_latest_readings(conn: sqlite3.Connection, building_id: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Latest point per unit_id + sensor_type (one row per sensor type per unit).
    """
    query = """
    WITH latest AS (
        SELECT unit_id, sensor_type, MAX(timestamp) AS ts
        FROM sensor_readings
        WHERE building_id = ?
        GROUP BY unit_id, sensor_type
    )
    SELECT sr.unit_id, sr.sensor_type, sr.timestamp, sr.value
    FROM sensor_readings sr
    JOIN latest
      ON sr.unit_id = latest.unit_id
     AND sr.sensor_type = latest.sensor_type
     AND sr.timestamp = latest.ts
    WHERE sr.building_id = ?
    """

    rows = conn.execute(query, (building_id, building_id)).fetchall()
    data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for r in rows:
        data.setdefault(r["unit_id"], {})
        data[r["unit_id"]][r["sensor_type"]] = {
            "timestamp": r["timestamp"],
            "value": float(r["value"]),
        }

    return data


def insert_anomalies(conn: sqlite3.Connection, anomalies: List[Dict[str, Any]]) -> None:
    if not anomalies:
        return

    conn.executemany("""
        INSERT INTO anomalies_log
        (timestamp, building_id, unit_id, sensor_id,
         anomaly_type, value, severity, action_taken)
        VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
    """, [
        (
            a["timestamp"],
            a["building_id"],
            a["unit_id"],
            a["type"],
            a["value"],
            a["severity"],
            a["action"],
        )
        for a in anomalies
    ])
    conn.commit()


# =========================================================
# Agent 2 helpers
# =========================================================
def load_active_consumption_model(conn: sqlite3.Connection) -> Tuple[str, Any, Any, float]:
    """
    Returns: (model_id, model, scaler, confidence)
    confidence: tries to use metrics_json -> test.r2, else 0.5
    """
    row = conn.execute("""
        SELECT model_id, file_path, metrics_json
        FROM model_registry
        WHERE model_scope='global'
          AND model_task='consumption_forecast'
          AND is_active=1
        ORDER BY trained_at DESC
        LIMIT 1
    """).fetchone()

    if not row:
        raise RuntimeError(
            "No active global consumption model in model_registry (is_active=1). "
            "Run scripts/train_models.py first."
        )

    model_id, file_path, metrics_json = row
    p = Path(file_path)

    if not p.exists():
        raise RuntimeError(f"Active model file not found on disk: {file_path}")

    with open(p, "rb") as f:
        payload = pickle.load(f)

    model = payload["model"]
    scaler = payload["scaler"]

    conf = 0.5
    try:
        m = json.loads(metrics_json)
        if isinstance(m, dict):
            conf = float(m.get("test", {}).get("r2", conf))
            conf = max(0.0, min(1.0, conf))
    except Exception:
        pass

    return model_id, model, scaler, conf


def fetch_recent_series_for_unit(conn: sqlite3.Connection, unit_id: str, lookback: int = 48) -> List[Dict[str, Any]]:
    """
    Fetch last N aligned records for a unit:
    energy + occupancy + area + weather
    (This matches what your model expects)
    """
    q = """
    SELECT
        sr.timestamp,
        sr.building_id,
        sr.unit_id,
        sr.value AS energy,
        COALESCE(occ.value, 0.0) AS occupancy,
        COALESCE(u.area_m2_final, 50.0) AS area_m2,
        COALESCE(ew.temp_external, 0.0) AS temp_external,
        COALESCE(ew.wind_speed_kmh, 0.0) AS wind_speed_kmh,
        COALESCE(ew.cloud_cover, 0.0) AS cloud_cover
    FROM sensor_readings sr
    JOIN units u ON u.unit_id = sr.unit_id
    JOIN buildings b ON b.building_id = sr.building_id
    LEFT JOIN sensor_readings occ
      ON occ.building_id = sr.building_id
     AND occ.unit_id = sr.unit_id
     AND occ.timestamp = sr.timestamp
     AND occ.sensor_type = 'occupancy'
     AND occ.quality_flag = 'ok'
    LEFT JOIN external_weather ew
      ON ew.location_id = b.location_id
     AND ew.timestamp = sr.timestamp
    WHERE sr.sensor_type='energy'
      AND sr.quality_flag='ok'
      AND sr.unit_id=?
    ORDER BY sr.timestamp DESC
    LIMIT ?
    """

    rows = conn.execute(q, (unit_id, lookback)).fetchall()
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for r in reversed(rows):  # chronological
        out.append({
            "timestamp": r["timestamp"],
            "building_id": r["building_id"],
            "unit_id": r["unit_id"],
            "energy": float(r["energy"]),
            "occupancy": float(r["occupancy"]),
            "area_m2": float(r["area_m2"]),
            "temp_external": float(r["temp_external"]),
            "wind_speed_kmh": float(r["wind_speed_kmh"]),
            "cloud_cover": float(r["cloud_cover"]),
        })
    return out


def insert_predictions_rows(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    conn.executemany("""
        INSERT INTO predictions (
            timestamp_created, timestamp_target,
            building_id, unit_id,
            predicted_consumption, predicted_occupancy_prob,
            model_name, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            r["timestamp_created"],
            r["timestamp_target"],
            r["building_id"],
            r["unit_id"],
            r["predicted_consumption"],
            r["predicted_occupancy_prob"],
            r["model_name"],
            r["confidence"],
        )
        for r in rows
    ])
    conn.commit()
