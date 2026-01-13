# utils/db_helper.py
import sqlite3
import json
import pickle
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


def connect(timeout: int = 30) -> sqlite3.Connection:
    """
    Central DB connection. Timeout helps avoid 'database is locked'.
    WAL helps concurrency (multiple reads + writes).
    """
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        return default


# =========================================================
# Agent 1 helpers
# =========================================================
def get_latest_readings(conn: sqlite3.Connection, building_id: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Latest point per unit_id + sensor_type (one row per sensor type per unit).
    Returns:
      { unit_id: { sensor_type: {timestamp, value} } }
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


def get_recent_readings(
    conn,
    building_id: str,
    sensor_types: list[str],
    lookback_hours: int,
    anchor_ts: str,   # ISO string iz baze/state-a
):
    placeholders = ",".join("?" for _ in sensor_types)
    query = f"""
SELECT unit_id, sensor_type, timestamp, value
FROM sensor_readings
WHERE building_id = ?
  AND sensor_type IN ({placeholders})
  AND datetime(replace(replace(timestamp,'T',' '),'Z','')) >= datetime(replace(replace(?,'T',' '),'Z',''), ?)
  AND datetime(replace(replace(timestamp,'T',' '),'Z','')) <= datetime(replace(replace(?,'T',' '),'Z',''))
ORDER BY unit_id, sensor_type, timestamp
"""

    hours_expr = f"-{int(lookback_hours)} hours"
    params = [building_id, *sensor_types, anchor_ts, hours_expr, anchor_ts]

    rows = conn.execute(query, params).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["unit_id"], {}).setdefault(r["sensor_type"], []).append((r["timestamp"], float(r["value"])))
    return out

def get_latest_readings_asof(conn, building_id: str, anchor_ts: str):
    query = """
    WITH latest AS (
        SELECT unit_id, sensor_type, MAX(timestamp) AS ts
        FROM sensor_readings
        WHERE building_id = ?
          AND datetime(timestamp) <= datetime(?)
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
    rows = conn.execute(query, (building_id, anchor_ts, building_id)).fetchall()
    data = {}
    for r in rows:
        data.setdefault(r["unit_id"], {})
        data[r["unit_id"]][r["sensor_type"]] = {
            "timestamp": r["timestamp"],
            "value": float(r["value"])
        }
    return data

def insert_anomalies(conn: sqlite3.Connection, anomalies: List[Dict[str, Any]]) -> None:
    """
    Insert anomalies into anomalies_log.
    Expected anomaly keys:
      timestamp, building_id, unit_id, type, value, severity, action
    """
    if not anomalies:
        return

    conn.executemany(
        """
        INSERT INTO anomalies_log
        (timestamp, building_id, unit_id, sensor_id,
         anomaly_type, value, severity, action_taken)
        VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
        """,
        [
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
        ],
    )
    conn.commit()


# =========================================================
# Agent 2 helpers
# =========================================================
def load_active_consumption_model(conn: sqlite3.Connection) -> Tuple[str, Any, Any, float]:
    """
    Loads ACTIVE global consumption model from model_registry and disk.

    Returns:
      (model_id, model, scaler, confidence)

    confidence:
      tries to use metrics_json -> test.r2 (clamped 0..1), else 0.5
    """
    row = conn.execute(
        """
        SELECT model_id, file_path, metrics_json
        FROM model_registry
        WHERE model_scope='global'
          AND model_task='consumption_forecast'
          AND is_active=1
        ORDER BY trained_at DESC
        LIMIT 1
        """
    ).fetchone()

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
            conf = float(m.get("confidence_score", m.get("test", {}).get("r2", conf)))
            conf = max(0.0, min(1.0, conf))
    except Exception:
        pass

    return model_id, model, scaler, conf


def fetch_recent_series_for_unit(
    conn: sqlite3.Connection,
    unit_id: str,
    lookback: int = 48
) -> List[Dict[str, Any]]:
    """
    Fetch last N aligned records for a unit:
      energy + occupancy + area + weather

    This matches what your global consumption model expects.
    Returns list chronological (old -> new).
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
        out.append(
            {
                "timestamp": r["timestamp"],
                "building_id": r["building_id"],
                "unit_id": r["unit_id"],
                "energy": float(r["energy"]),
                "occupancy": float(r["occupancy"]),
                "area_m2": float(r["area_m2"]),
                "temp_external": float(r["temp_external"]),
                "wind_speed_kmh": float(r["wind_speed_kmh"]),
                "cloud_cover": float(r["cloud_cover"]),
            }
        )
    return out

def fetch_recent_series_for_unit_asof(
    conn: sqlite3.Connection,
    unit_id: str,
    anchor_ts: str,
    lookback: int = 48
) -> List[Dict[str, Any]]:
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
      AND datetime(sr.timestamp) <= datetime(?)
    ORDER BY sr.timestamp DESC
    LIMIT ?
    """
    rows = conn.execute(q, (unit_id, anchor_ts, lookback)).fetchall()
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for r in reversed(rows):
        out.append(
            {
                "timestamp": r["timestamp"],
                "building_id": r["building_id"],
                "unit_id": r["unit_id"],
                "energy": float(r["energy"]),
                "occupancy": float(r["occupancy"]),
                "area_m2": float(r["area_m2"]),
                "temp_external": float(r["temp_external"]),
                "wind_speed_kmh": float(r["wind_speed_kmh"]),
                "cloud_cover": float(r["cloud_cover"]),
            }
        )
    return out 

def insert_predictions_rows(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> None:
    """
    Inserts rows into predictions table.

    Expected keys:
      timestamp_created, timestamp_target, building_id, unit_id,
      predicted_consumption, predicted_occupancy_prob,
      model_name, confidence
    """
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO predictions (
            timestamp_created, timestamp_target,
            building_id, unit_id,
            predicted_consumption, predicted_occupancy_prob,
            model_name, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
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
        ],
    )
    conn.commit()


# =========================================================
# Agent 3 helpers
# =========================================================
def get_unit_cluster(conn: sqlite3.Connection, building_id: str, unit_id: str) -> Optional[str]:
    """
    Returns latest cluster_id for unit (if clustering has been run).
    """
    row = conn.execute(
        """
        SELECT cluster_id
        FROM unit_cluster_assignment
        WHERE building_id = ?
          AND unit_id = ?
        ORDER BY start_date DESC
        LIMIT 1
        """,
        (building_id, unit_id),
    ).fetchone()

    return row[0] if row else None


def get_tariff_for_building(conn: sqlite3.Connection, building_id: str) -> Dict[str, Any]:
    """
    Reads tariff_model (1 row per building).
    If missing, returns defaults.
    """
    row = conn.execute(
        """
        SELECT low_tariff_start, low_tariff_end,
               low_price_per_kwh, high_price_per_kwh,
               sunday_all_day_low, currency
        FROM tariff_model
        WHERE building_id = ?
        LIMIT 1
        """,
        (building_id,),
    ).fetchone()

    if not row:
        return {
            "low_tariff_start": "22:00",
            "low_tariff_end": "06:00",
            "low_price_per_kwh": 0.08,
            "high_price_per_kwh": 0.18,
            "sunday_all_day_low": 1,
            "currency": "BAM",
        }

    return {
        "low_tariff_start": row["low_tariff_start"],
        "low_tariff_end": row["low_tariff_end"],
        "low_price_per_kwh": float(row["low_price_per_kwh"]),
        "high_price_per_kwh": float(row["high_price_per_kwh"]),
        "sunday_all_day_low": int(row["sunday_all_day_low"]),
        "currency": row["currency"],
    }


def _time_to_minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def get_price_for_timestamp(tariff: Dict[str, Any], ts_iso: str) -> float:
    """
    Given tariff dict + ISO timestamp, returns low/high price.
    Handles wrap like 22:00 -> 06:00 and Sunday all-day low.
    """
    dt = datetime.fromisoformat(ts_iso.replace("Z", ""))

    # Sunday all-day low?
    if int(tariff.get("sunday_all_day_low", 1)) == 1 and dt.weekday() == 6:
        return float(tariff["low_price_per_kwh"])

    start = _time_to_minutes(tariff["low_tariff_start"])
    end = _time_to_minutes(tariff["low_tariff_end"])
    cur = dt.hour * 60 + dt.minute

    # wrap case (start > end)
    if start <= end:
        is_low = start <= cur < end
    else:
        is_low = (cur >= start) or (cur < end)

    return float(tariff["low_price_per_kwh"] if is_low else tariff["high_price_per_kwh"])


def insert_optimization_plans(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> None:
    """
    Bulk insert into optimization_plans table.

    Expected keys:
      timestamp, building_id, unit_id,
      action_type, target_temp,
      start_time, end_time,
      estimated_cost, estimated_savings,
      method (optional)
    """
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO optimization_plans (
            timestamp,
            building_id,
            unit_id,
            action_type,
            target_temp,
            start_time,
            end_time,
            estimated_cost,
            estimated_savings,
            method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["timestamp"],
                r["building_id"],
                r["unit_id"],
                r["action_type"],
                r["target_temp"],
                r.get("start_time"),
                r.get("end_time"),
                r.get("estimated_cost"),
                r.get("estimated_savings"),
                r.get("method", "heuristic_v1"),
            )
            for r in rows
        ],
    )
    conn.commit()
    
def ensure_pipeline_progress(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_progress (
      pipeline_name TEXT NOT NULL,
      building_id   TEXT NOT NULL,
      current_anchor_ts TEXT NOT NULL,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (pipeline_name, building_id)
    );
    """)
    conn.commit()

def get_latest_timestamp(conn, building_id: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(timestamp) AS ts FROM sensor_readings WHERE building_id=?",
        (building_id,),
    ).fetchone()
    return row["ts"] if row and row["ts"] else None

def get_all_building_ids(conn) -> list[str]:
    rows = conn.execute("SELECT building_id FROM buildings ORDER BY building_id").fetchall()
    return [r[0] for r in rows]

def get_or_init_anchor(conn, pipeline_name: str, building_id: str) -> str:
    row = conn.execute(
        "SELECT current_anchor_ts FROM pipeline_progress WHERE pipeline_name=? AND building_id=?",
        (pipeline_name, building_id),
    ).fetchone()

    if row:
        return row[0]

    latest = get_latest_timestamp(conn, building_id)
    if not latest:
        raise RuntimeError(f"No sensor_readings for building_id={building_id}")

    conn.execute(
        "INSERT INTO pipeline_progress(pipeline_name, building_id, current_anchor_ts) VALUES (?, ?, ?)",
        (pipeline_name, building_id, latest),
    )
    conn.commit()
    return latest


def step_anchor_back(conn, pipeline_name: str, building_id: str, hours: int = 24) -> str:
    from datetime import datetime, timezone, timedelta

    anchor = get_or_init_anchor(conn, pipeline_name, building_id)
    dt = datetime.fromisoformat(anchor.replace("Z", "+00:00"))
    new_dt = dt - timedelta(hours=hours)
    new_anchor = new_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    conn.execute(
        "UPDATE pipeline_progress SET current_anchor_ts=?, updated_at=CURRENT_TIMESTAMP WHERE pipeline_name=? AND building_id=?",
        (new_anchor, pipeline_name, building_id),
    )
    conn.commit()
    return new_anchor

def insert_decisions_rows(conn, rows: list[dict]) -> None:
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO decisions_log (
            timestamp, building_id, unit_id,
            action, approved,
            reasoning_text, confidence, mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["timestamp"],
                r["building_id"],
                r["unit_id"],
                r["action"],
                int(r.get("approved", 1)),
                r.get("reasoning_text"),
                float(r.get("confidence", 0.6)),
                r.get("mode", "learning"),
            )
            for r in rows
        ],
    )
    conn.commit()

def get_sensor_id(conn: sqlite3.Connection, unit_id: str, sensor_type: str) -> Optional[str]:
    row = conn.execute(
        "SELECT sensor_id FROM sensors WHERE unit_id=? AND sensor_type=? LIMIT 1",
        (unit_id, sensor_type),
    ).fetchone()
    return row["sensor_id"] if row else None


def ensure_validation_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_validation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            building_id TEXT NOT NULL,
            status TEXT NOT NULL,
            model_confidence_avg REAL,
            coverage REAL,
            blocked_units_count INTEGER,
            invalid_units_count INTEGER,
            reasons_json TEXT
        )
        """
    )
    conn.commit()


def insert_validation_log(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    ensure_validation_log(conn)
    conn.execute(
        """
        INSERT INTO system_validation_log (
            timestamp, building_id, status,
            model_confidence_avg, coverage,
            blocked_units_count, invalid_units_count,
            reasons_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["timestamp"],
            row["building_id"],
            row["status"],
            row.get("model_confidence_avg"),
            row.get("coverage"),
            row.get("blocked_units_count"),
            row.get("invalid_units_count"),
            row.get("reasons_json"),
        ),
    )
    conn.commit()

