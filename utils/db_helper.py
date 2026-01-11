import sqlite3
from pathlib import Path
from typing import Dict, Any

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
    data = {}

    for r in rows:
        data.setdefault(r["unit_id"], {})
        data[r["unit_id"]][r["sensor_type"]] = {
            "timestamp": r["timestamp"],
            "value": float(r["value"])
        }

    return data


def insert_anomalies(conn, anomalies):
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
            a["action"]
        )
        for a in anomalies
    ])
    conn.commit()
