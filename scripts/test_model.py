import sqlite3
import pickle
import json
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"
EXPORT_DIR = BASE_DIR / "exports"
EXPORT_DIR.mkdir(exist_ok=True)

LOOKBACK = 48  # 24h (30-min)
HORIZON = 1    # next 30-min step


def parse_dt(ts: str) -> datetime:
    ts_norm = ts.replace("T", " ").replace("Z", "")
    return datetime.fromisoformat(ts_norm)


def load_active_model(conn: sqlite3.Connection):
    row = conn.execute("""
        SELECT model_id, file_path, metrics_json, trained_at
        FROM model_registry
        WHERE model_scope='global'
          AND model_task='consumption_forecast'
          AND is_active=1
        ORDER BY trained_at DESC
        LIMIT 1
    """).fetchone()

    if not row:
        raise RuntimeError("No active model found in model_registry.")

    model_id, file_path, metrics_json, trained_at = row
    model_file = Path(file_path)

    if not model_file.exists():
        raise RuntimeError(f"Model file not found on disk: {file_path}")

    with open(model_file, "rb") as f:
        payload = pickle.load(f)

    model = payload["model"]
    scaler = payload["scaler"]

    try:
        metrics = json.loads(metrics_json)
    except Exception:
        metrics = {"raw": metrics_json}

    return model_id, model_file, model, scaler, metrics, trained_at


def fetch_unit_energy_series(conn: sqlite3.Connection, unit_id: str):
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

    by_ts = {}
    for r in rows:
        ts, building_id, unit_id, energy, occ, area, t_ext, wind, cloud = r
        by_ts[ts] = {
            "timestamp": ts,
            "building_id": building_id,
            "unit_id": unit_id,
            "energy": float(energy),
            "occupancy": float(occ),
            "area_m2": float(area),
            "temp_external": float(t_ext),
            "wind_speed_kmh": float(wind),
            "cloud_cover": float(cloud),
        }

    records = list(by_ts.values())
    records.sort(key=lambda x: x["timestamp"])
    return records


def build_features_for_index(records, idx):
    """
    Build the same 17-feature vector as in training for target at records[idx]
    using history window [idx-LOOKBACK, idx)
    """
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
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    return {"mae": mae, "rmse": rmse, "r2": r2}


def write_csv(path: Path, rows: list):
    import csv
    if not rows:
        return
    keys = rows[0].keys()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def main():
    conn = sqlite3.connect(str(DB_PATH))

    model_id, model_file, model, scaler, registry_metrics, trained_at = load_active_model(conn)
    print("=" * 80)
    print("MODEL TEST")
    print("=" * 80)
    print(f"Active model_id : {model_id}")
    print(f"Model file      : {model_file}")
    print(f"Trained at      : {trained_at}")
    print("Registry metrics (summary keys):", list(registry_metrics.keys()))
    print("-" * 80)

    unit_rows = conn.execute("""
        SELECT DISTINCT unit_id, building_id
        FROM units
        ORDER BY building_id, unit_id
    """).fetchall()

    if not unit_rows:
        print("[ERROR] No units found.")
        return

    chosen = []
    by_building = defaultdict(list)
    for unit_id, building_id in unit_rows:
        by_building[building_id].append(unit_id)

    for b, units in by_building.items():
        chosen.extend(units[:3])

    chosen = chosen[:6]
    print(f"Testing units: {chosen}")
    print("-" * 80)

    all_true = []
    all_pred = []

    for unit_id in chosen:
        records = fetch_unit_energy_series(conn, unit_id)
        if len(records) < LOOKBACK + HORIZON + 10:
            print(f"[WARN] {unit_id}: not enough records ({len(records)})")
            continue

        # take last N points for evaluation
        N = min(336, len(records) - LOOKBACK - HORIZON)
        start_idx = len(records) - N

        y_true = []
        y_pred = []

        export_rows = []

        for idx in range(start_idx, len(records)):
            if idx < LOOKBACK:
                continue

            # our target is "current energy at idx" and features are from previous LOOKBACK
            # this matches training where y was target["energy"]
            x = build_features_for_index(records, idx)
            xs = scaler.transform([x])
            pred = float(model.predict(xs)[0])

            true = float(records[idx]["energy"])

            y_true.append(true)
            y_pred.append(pred)

            export_rows.append({
                "timestamp": records[idx]["timestamp"],
                "unit_id": unit_id,
                "actual_energy": round(true, 4),
                "predicted_energy": round(pred, 4),
                "error": round(pred - true, 4),
                "abs_error": round(abs(pred - true), 4),
            })

        m = metrics(y_true, y_pred)
        print(f"{unit_id}: MAE={m['mae']:.4f} RMSE={m['rmse']:.4f} R²={m['r2']:.4f} (n={len(y_true)})")

        out = EXPORT_DIR / f"pred_vs_actual_{unit_id}.csv"
        write_csv(out, export_rows)
        print(f"CSV saved: {out}")

        all_true.extend(y_true)
        all_pred.extend(y_pred)

    if all_true:
        m_all = metrics(all_true, all_pred)
        print("-" * 80)
        print(f"Overall (chosen units): MAE={m_all['mae']:.4f} RMSE={m_all['rmse']:.4f} R²={m_all['r2']:.4f} (n={len(all_true)})")
        print(f"Exports folder: {EXPORT_DIR}")
    else:
        print("[ERROR] No evaluation points produced.")

    conn.close()


if __name__ == "__main__":
    main()
