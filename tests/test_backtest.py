# tests/test_backtest.py
import sqlite3
import pickle
from pathlib import Path
from datetime import datetime
import unittest

import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"

LOOKBACK = 48
HORIZON = 1


def parse_dt(ts: str) -> datetime:
    ts_norm = ts.replace("T", " ").replace("Z", "")
    return datetime.fromisoformat(ts_norm)


def load_active_model(conn: sqlite3.Connection):
    row = conn.execute(
        """
        SELECT model_id, file_path
        FROM model_registry
        WHERE model_scope='global'
          AND model_task='consumption_forecast'
          AND is_active=1
        ORDER BY trained_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        raise RuntimeError("No active model found in model_registry.")

    model_id, file_path = row
    model_file = Path(file_path)
    if not model_file.exists():
        raise RuntimeError(f"Model file not found on disk: {file_path}")

    with open(model_file, "rb") as f:
        payload = pickle.load(f)
    return model_id, payload["model"], payload["scaler"]


def fetch_unit_records(conn: sqlite3.Connection, unit_id: str):
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
    ORDER BY sr.timestamp
    """
    rows = conn.execute(q, (unit_id,)).fetchall()
    if not rows:
        return []

    out = []
    for r in rows:
        out.append(
            {
                "timestamp": r[0],
                "building_id": r[1],
                "unit_id": r[2],
                "energy": float(r[3]),
                "occupancy": float(r[4]),
                "area_m2": float(r[5]),
                "temp_external": float(r[6]),
                "wind_speed_kmh": float(r[7]),
                "cloud_cover": float(r[8]),
            }
        )
    return out


def build_features(records, idx):
    seq = records[idx - LOOKBACK: idx]
    target = records[idx]

    e_seq = np.array([r["energy"] for r in seq], dtype=float)
    o_seq = np.array([r["occupancy"] for r in seq], dtype=float)
    t_seq = np.array([r["temp_external"] for r in seq], dtype=float)

    dt = parse_dt(target["timestamp"])
    hour = dt.hour
    dow = dt.weekday()
    is_weekend = 1 if dow >= 5 else 0

    feats = [
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
    ]
    return np.array(feats, dtype=float)


def metrics(y_true, y_pred):
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)
    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    return {"mae": mae, "rmse": rmse}


class TestModelBacktest(unittest.TestCase):
    def test_backtest_recent_points(self):
        conn = sqlite3.connect(str(DB_PATH))
        try:
            _, model, scaler = load_active_model(conn)

            units = conn.execute(
                "SELECT unit_id FROM units ORDER BY unit_id LIMIT 6"
            ).fetchall()
            self.assertTrue(units, "No units found for backtest.")

            all_true = []
            all_pred = []

            for (unit_id,) in units:
                records = fetch_unit_records(conn, unit_id)
                if len(records) < LOOKBACK + HORIZON + 20:
                    continue

                start_idx = len(records) - 200
                for idx in range(start_idx, len(records)):
                    if idx < LOOKBACK:
                        continue
                    x = build_features(records, idx)
                    xs = scaler.transform([x])
                    pred = float(model.predict(xs)[0])
                    true = float(records[idx]["energy"])
                    all_true.append(true)
                    all_pred.append(pred)

            self.assertTrue(all_true, "No evaluation points produced.")
            m = metrics(all_true, all_pred)

            # Sim data should be reasonably predictable; thresholds are lenient.
            self.assertLess(m["mae"], 1.2, f"MAE too high: {m['mae']:.3f}")
            self.assertLess(m["rmse"], 1.8, f"RMSE too high: {m['rmse']:.3f}")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
