import sqlite3
import numpy as np
import pickle
import json
import time
from datetime import datetime
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"
MODELS_DIR = BASE_DIR / "models"
MODELS_DIR.mkdir(exist_ok=True)

FEATURE_VERSION = 3

LOG_EVERY_ROWS = 10000       # print progress every N fetched rows
FETCHMANY_SIZE = 5000        # fetch rows in chunks 


def ts():
    return datetime.now().strftime("%H:%M:%S")


def ensure_model_registry(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS model_registry (
        model_id TEXT PRIMARY KEY,
        model_scope TEXT NOT NULL,
        model_task TEXT NOT NULL,
        model_type TEXT NOT NULL,
        feature_version INTEGER NOT NULL,
        trained_at TEXT NOT NULL,
        file_path TEXT NOT NULL,
        metrics_json TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 0
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_model_registry_active ON model_registry(is_active, model_task)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_model_registry_scope ON model_registry(model_scope, model_task)")
    conn.commit()


def ensure_indexes(conn: sqlite3.Connection):
    print(f"[{ts()}] Ensuring indexes...")
    conn.executescript("""
    CREATE INDEX IF NOT EXISTS idx_sr_main
      ON sensor_readings(sensor_type, quality_flag, building_id, unit_id, timestamp);

    CREATE INDEX IF NOT EXISTS idx_sr_occ
      ON sensor_readings(building_id, unit_id, timestamp, sensor_type, quality_flag);

    CREATE INDEX IF NOT EXISTS idx_ew_loc_ts
      ON external_weather(location_id, timestamp);

    CREATE INDEX IF NOT EXISTS idx_bld_loc
      ON buildings(building_id, location_id);
    """)
    conn.commit()
    # ANALYZE helps sqlite query planner
    conn.execute("ANALYZE;")
    conn.commit()
    print(f"[{ts()}] ✅ Indexes OK")


def quick_counts(conn: sqlite3.Connection):
    q1 = "SELECT COUNT(*) FROM sensor_readings WHERE sensor_type='energy' AND quality_flag='ok'"
    q2 = "SELECT COUNT(*) FROM sensor_readings WHERE sensor_type='occupancy' AND quality_flag='ok'"
    q3 = "SELECT COUNT(*) FROM external_weather"
    e = conn.execute(q1).fetchone()[0]
    o = conn.execute(q2).fetchone()[0]
    w = conn.execute(q3).fetchone()[0]
    print(f"[{ts()}] Counts: energy_ok={e} occupancy_ok={o} external_weather={w}")


def profile_fetch_query(conn: sqlite3.Connection):
    print(f"[{ts()}] Profiling JOINs with LIMIT 10...")

    tests = [
        ("energy only",
         """SELECT sr.timestamp, sr.unit_id, sr.value
            FROM sensor_readings sr
            WHERE sr.sensor_type='energy' AND sr.quality_flag='ok'
            LIMIT 10"""),
        ("energy + buildings",
         """SELECT sr.timestamp, sr.unit_id, sr.value, b.location_id
            FROM sensor_readings sr
            JOIN buildings b ON b.building_id = sr.building_id
            WHERE sr.sensor_type='energy' AND sr.quality_flag='ok'
            LIMIT 10"""),
        ("energy + occupancy join",
         """SELECT sr.timestamp, sr.unit_id, sr.value, COALESCE(occ.value,0)
            FROM sensor_readings sr
            LEFT JOIN sensor_readings occ
              ON occ.building_id=sr.building_id
             AND occ.unit_id=sr.unit_id
             AND occ.timestamp=sr.timestamp
             AND occ.sensor_type='occupancy'
             AND occ.quality_flag='ok'
            WHERE sr.sensor_type='energy' AND sr.quality_flag='ok'
            LIMIT 10"""),
        ("energy + weather join",
         """SELECT sr.timestamp, sr.unit_id, sr.value, COALESCE(ew.temp_external,0)
            FROM sensor_readings sr
            JOIN buildings b ON b.building_id=sr.building_id
            LEFT JOIN external_weather ew
              ON ew.location_id=b.location_id
             AND ew.timestamp=sr.timestamp
            WHERE sr.sensor_type='energy' AND sr.quality_flag='ok'
            LIMIT 10"""),
        ("full join",
         """SELECT sr.timestamp, sr.building_id, sr.unit_id, sr.value,
                   COALESCE(occ.value,0),
                   COALESCE(u.area_m2_final,50),
                   COALESCE(ew.temp_external,0),
                   COALESCE(ew.wind_speed_kmh,0),
                   COALESCE(ew.cloud_cover,0)
            FROM sensor_readings sr
            JOIN units u ON u.unit_id=sr.unit_id
            JOIN buildings b ON b.building_id=sr.building_id
            LEFT JOIN sensor_readings occ
              ON occ.building_id=sr.building_id
             AND occ.unit_id=sr.unit_id
             AND occ.timestamp=sr.timestamp
             AND occ.sensor_type='occupancy'
             AND occ.quality_flag='ok'
            LEFT JOIN external_weather ew
              ON ew.location_id=b.location_id
             AND ew.timestamp=sr.timestamp
            WHERE sr.sensor_type='energy' AND sr.quality_flag='ok'
            LIMIT 10""")
    ]

    for name, q in tests:
        t0 = time.time()
        rows = conn.execute(q).fetchall()
        dt = time.time() - t0
        print(f"[{ts()}]   - {name}: {dt:.3f}s (rows={len(rows)})")


def fetch_global_timeseries(conn: sqlite3.Connection):
    query = """
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

    WHERE sr.sensor_type = 'energy'
      AND sr.quality_flag = 'ok'
    ORDER BY sr.unit_id, sr.timestamp
    """

    print(f"[{ts()}] Executing main fetch query (streaming)...")
    t0 = time.time()

    cursor = conn.execute(query)

    data_by_unit = {}
    total = 0

    while True:
        batch = cursor.fetchmany(FETCHMANY_SIZE)
        if not batch:
            break

        for (ts_, building_id, unit_id, energy, occ, area, t_ext, wind, cloud) in batch:
            if unit_id not in data_by_unit:
                data_by_unit[unit_id] = []
            data_by_unit[unit_id].append({
                "timestamp": ts_,
                "building_id": building_id,
                "unit_id": unit_id,
                "energy": float(energy),
                "occupancy": float(occ),
                "area_m2": float(area),
                "temp_external": float(t_ext),
                "wind_speed_kmh": float(wind),
                "cloud_cover": float(cloud),
            })

        total += len(batch)
        if total % LOG_EVERY_ROWS == 0:
            print(f"[{ts()}]   ... fetched rows={total:,} units={len(data_by_unit)} elapsed={time.time()-t0:.1f}s")

    dt = time.time() - t0
    print(f"[{ts()}] Fetch done: rows={total:,} units={len(data_by_unit)} elapsed={dt:.2f}s")

    if total == 0:
        return None
    return data_by_unit


def build_supervised_rows(data_by_unit, lookback=48, horizon=1):
    X, y, meta = [], [], []

    t0 = time.time()
    unit_count = 0

    for unit_id, records in data_by_unit.items():
        if len(records) < lookback + horizon:
            continue
        unit_count += 1

        records = sorted(records, key=lambda r: r["timestamp"])
        energies = np.array([r["energy"] for r in records], dtype=float)
        occs = np.array([r["occupancy"] for r in records], dtype=float)
        temps = np.array([r["temp_external"] for r in records], dtype=float)

        n_samples = len(records) - lookback - horizon + 1
        for i in range(n_samples):
            seq_slice = slice(i, i + lookback)
            target_idx = i + lookback + (horizon - 1)
            target = records[target_idx]

            e_seq = energies[seq_slice]
            o_seq = occs[seq_slice]
            t_seq = temps[seq_slice]

            ts_norm = target["timestamp"].replace("T", " ").replace("Z", "")
            dt = datetime.fromisoformat(ts_norm)

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

            X.append(feats)
            y.append(float(target["energy"]))
            meta.append({
                "timestamp": target["timestamp"],
                "unit_id": unit_id,
                "building_id": target["building_id"],
            })

        if unit_count % 10 == 0:
            print(f"[{ts()}]   ... processed units={unit_count} samples_so_far={len(X):,} elapsed={time.time()-t0:.1f}s")

    if not X:
        return None, None, None

    print(f"[{ts()}] Dataset built: units_used={unit_count} samples={len(X):,} elapsed={time.time()-t0:.2f}s")
    return np.array(X, dtype=float), np.array(y, dtype=float), meta


def time_based_split(X, y, meta, test_ratio=0.2):
    idx = list(range(len(meta)))
    idx.sort(key=lambda i: meta[i]["timestamp"])
    n = len(idx)
    split = int((1.0 - test_ratio) * n)
    train_idx = idx[:split]
    test_idx = idx[split:]
    return X[train_idx], X[test_idx], y[train_idx], y[test_idx]


def train_model(X_train, y_train, model_type="random_forest"):
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)

    if model_type == "random_forest":
        model = RandomForestRegressor(
            n_estimators=300,
            max_depth=18,
            min_samples_split=5,
            random_state=42,
            n_jobs=-1,
        )
    else:
        model = GradientBoostingRegressor(
            n_estimators=250,
            max_depth=5,
            learning_rate=0.05,
            random_state=42,
        )

    model.fit(X_train_scaled, y_train)
    return model, scaler


def eval_model(model, scaler, X, y):
    Xs = scaler.transform(X)
    pred = model.predict(Xs)
    return {
        "mse": float(mean_squared_error(y, pred)),
        "rmse": float(np.sqrt(mean_squared_error(y, pred))),
        "mae": float(mean_absolute_error(y, pred)),
        "r2": float(r2_score(y, pred)),
    }


def eval_baseline_persistence(X, y):
    baseline_pred = X[:, 4]
    return {
        "mse": float(mean_squared_error(y, baseline_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y, baseline_pred))),
        "mae": float(mean_absolute_error(y, baseline_pred)),
        "r2": float(r2_score(y, baseline_pred)),
    }


def save_model(model, scaler, model_type, metrics):
    trained_at = datetime.utcnow().isoformat() + "Z"
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"global_consumption_{model_type}_{stamp}.pkl"
    filepath = MODELS_DIR / filename

    payload = {
        "scope": "global",
        "task": "consumption_forecast",
        "model_type": model_type,
        "trained_at": trained_at,
        "feature_version": FEATURE_VERSION,
        "model": model,
        "scaler": scaler,
        "metrics": metrics,
    }

    with open(filepath, "wb") as f:
        pickle.dump(payload, f)

    return str(filepath), trained_at


def register_model_in_db(conn, file_path, trained_at, model_type, metrics):
    ensure_model_registry(conn)

    model_id = f"global_consumption_{model_type}_{trained_at}"
    metrics_json = json.dumps(metrics, ensure_ascii=False)

    conn.execute("""
        UPDATE model_registry
        SET is_active = 0
        WHERE model_scope = 'global'
          AND model_task = 'consumption_forecast'
          AND is_active = 1
    """)

    conn.execute("""
        INSERT INTO model_registry (
            model_id, model_scope, model_task, model_type,
            feature_version, trained_at, file_path, metrics_json, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, (
        model_id,
        "global",
        "consumption_forecast",
        model_type,
        FEATURE_VERSION,
        trained_at,
        file_path,
        metrics_json,
    ))
    conn.commit()
    return model_id


def run(db_path: str, model_type: str = "random_forest", lookback: int = 48, horizon: int = 1, test_ratio: float = 0.2):
    conn = sqlite3.connect(db_path)

    print("\n" + "=" * 70)
    print("GLOBAL TRAINING: Consumption Forecast Model (ALL buildings)")
    print("=" * 70)

    ensure_indexes(conn)
    quick_counts(conn)
    profile_fetch_query(conn)

    print(f"[{ts()}] 📥 Fetching global timeseries...")
    data_by_unit = fetch_global_timeseries(conn)
    if not data_by_unit:
        print("[ERROR] No training data found.")
        conn.close()
        return

    print(f"[{ts()}] Loaded units: {len(data_by_unit)}")

    print(f"[{ts()}] Building supervised dataset (sliding windows)...")
    X, y, meta = build_supervised_rows(data_by_unit, lookback=lookback, horizon=horizon)
    if X is None:
        print("[ERROR] Not enough data to create training samples.")
        conn.close()
        return

    print(f"[{ts()}] Samples: {len(X):,} | Features: {X.shape[1]} | Target: next-step energy")

    X_train, X_test, y_train, y_test = time_based_split(X, y, meta, test_ratio=test_ratio)
    print(f"[{ts()}] Time split: train={len(X_train):,} test={len(X_test):,} (test_ratio={test_ratio})")

    baseline_test = eval_baseline_persistence(X_test, y_test)
    print(f"[{ts()}] Baseline (persistence) TEST: MAE={baseline_test['mae']:.4f} RMSE={baseline_test['rmse']:.4f} R²={baseline_test['r2']:.4f}")

    print(f"[{ts()}] Training model: {model_type} ...")
    t0 = time.time()
    model, scaler = train_model(X_train, y_train, model_type=model_type)
    print(f"[{ts()}] Train done in {time.time()-t0:.2f}s")

    train_metrics = eval_model(model, scaler, X_train, y_train)
    test_metrics = eval_model(model, scaler, X_test, y_test)

    metrics = {
        "train": train_metrics,
        "test": test_metrics,
        "baseline_test_persistence": baseline_test,
        "config": {"lookback": lookback, "horizon": horizon, "test_ratio": test_ratio},
    }

    print("\n" + "=" * 70)
    print("MODEL PERFORMANCE")
    print("=" * 70)
    print(f"Train: MAE={train_metrics['mae']:.4f} RMSE={train_metrics['rmse']:.4f} R²={train_metrics['r2']:.4f}")
    print(f"Test : MAE={test_metrics['mae']:.4f} RMSE={test_metrics['rmse']:.4f} R²={test_metrics['r2']:.4f}")

    file_path, trained_at = save_model(model, scaler, model_type, metrics)
    print(f"\n[{ts()}] Saved model: {file_path}")

    model_id = register_model_in_db(conn, file_path, trained_at, model_type, metrics)
    print(f"[{ts()}] Registered model in DB as ACTIVE: {model_id}")

    print("\n Global training completed.\n")
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train a global consumption model across all buildings.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to database")
    parser.add_argument("--model", choices=["random_forest", "gradient_boosting"], default="random_forest")
    parser.add_argument("--lookback", type=int, default=48, help="History length (48=24h for 30-min data)")
    parser.add_argument("--horizon", type=int, default=1, help="Steps ahead (1=30-min ahead)")
    parser.add_argument("--test_ratio", type=float, default=0.2, help="Time-based test ratio")
    args = parser.parse_args()

    run(args.db, args.model, args.lookback, args.horizon, args.test_ratio)
