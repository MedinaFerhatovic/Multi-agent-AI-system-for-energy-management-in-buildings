# utils/db_helper.py
import sqlite3
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def get_latest_readings(conn, building_id: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
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
            "value": float(r["value"])
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


def get_latest_timestamp(conn, building_id: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(timestamp) FROM sensor_readings WHERE building_id=?",
        (building_id,)
    ).fetchone()
    return row[0] if row and row[0] else None


def get_sensor_id(conn, unit_id: str, sensor_type: str) -> Optional[str]:
    row = conn.execute(
        "SELECT sensor_id FROM sensors WHERE unit_id=? AND sensor_type=? LIMIT 1",
        (unit_id, sensor_type),
    ).fetchone()
    return row["sensor_id"] if row else None


def get_tariff_model(conn, building_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT low_tariff_start, low_tariff_end,
               low_price_per_kwh, high_price_per_kwh,
               sunday_all_day_low, currency
        FROM tariff_model
        WHERE building_id = ?
        """,
        (building_id,),
    ).fetchone()

    if not row:
        # safe defaults
        return {
            "low_tariff_start": "22:00",
            "low_tariff_end": "06:00",
            "low_price_per_kwh": 0.08,
            "high_price_per_kwh": 0.18,
            "sunday_all_day_low": 1,
            "currency": "BAM",
        }

    return dict(row)


def insert_anomalies(conn, anomalies: List[Dict[str, Any]]):
    """
    anomalies expected keys:
      timestamp, building_id, unit_id, type, value, severity, action
    optional:
      sensor_id
    """
    if not anomalies:
        return

    conn.executemany(
        """
        INSERT INTO anomalies_log
        (timestamp, building_id, unit_id, sensor_id,
         anomaly_type, value, severity, action_taken)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                a["timestamp"],
                a["building_id"],
                a["unit_id"],
                a.get("sensor_id"),
                a["type"],
                a.get("value"),
                a.get("severity", "medium"),
                a.get("action", "investigate"),
            )
            for a in anomalies
        ],
    )
    conn.commit()

def get_all_building_ids(conn) -> list[str]:
    rows = conn.execute("SELECT building_id FROM buildings ORDER BY building_id").fetchall()
    return [r[0] for r in rows]
