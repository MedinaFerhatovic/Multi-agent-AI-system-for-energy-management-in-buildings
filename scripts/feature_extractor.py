import sqlite3
from datetime import datetime, timedelta, date
import math
from typing import Optional, Tuple, List, Dict

DEFAULT_DB_PATH = "db/smartbuilding.db"

MORNING_START = 6
MORNING_END = 8          # 06:00-07:59

DAY_START = 8
DAY_END = 17             # 08:00-16:59

EVENING_START = 17
EVENING_END = 22         # 17:00-21:59

NIGHT_START = 22
NIGHT_END = 6            # 22:00-05:59

# Peak windows
PEAK_MORNING_START = 6
PEAK_MORNING_END = 12
PEAK_EVENING_START = 16
PEAK_EVENING_END = 22

# Rounding policy
ROUND_OCC = 2
ROUND_ENERGY = 2
ROUND_STD = 2
ROUND_RATIO = 3
ROUND_CORR = 3


def rround(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def pearson_abs(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    deny = math.sqrt(sum((y - my) ** 2 for y in ys))
    if denx == 0 or deny == 0:
        return None
    return abs(num / (denx * deny))


def std_dev(vals: List[float]) -> Optional[float]:
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / len(vals)
    return math.sqrt(var)


def normalize_ts(ts: str) -> str:
    return ts.replace("T", " ") if ts else ts


def hour_of(ts_str: str) -> int:
    ts_str = normalize_ts(ts_str)
    return int(ts_str[11:13])


def day_of_week(d: str) -> int:
    y, m, dd = map(int, d.split("-"))
    return date(y, m, dd).weekday()  


def fetch_units(conn: sqlite3.Connection, building_id: str) -> List[str]:
    cur = conn.execute("SELECT unit_id FROM units WHERE building_id = ?", (building_id,))
    return [r[0] for r in cur.fetchall()]


def fetch_days_range(conn: sqlite3.Connection, building_id: str) -> Tuple[Optional[str], Optional[str]]:
    cur = conn.execute(
        "SELECT MIN(date(timestamp)), MAX(date(timestamp)) "
        "FROM sensor_readings WHERE building_id = ?",
        (building_id,),
    )
    return cur.fetchone()


def fetch_building_location_id(conn: sqlite3.Connection, building_id: str) -> Optional[str]:
    cur = conn.execute("SELECT location_id FROM buildings WHERE building_id = ?", (building_id,))
    row = cur.fetchone()
    return row[0] if row else None


def fetch_readings_for_day(conn: sqlite3.Connection, building_id: str, unit_id: str, day: str) -> Dict[str, List[Tuple[str, float]]]:
    cur = conn.execute(
        "SELECT timestamp, sensor_type, value "
        "FROM sensor_readings "
        "WHERE building_id = ? AND unit_id = ? AND date(timestamp) = ?",
        (building_id, unit_id, day),
    )
    out: Dict[str, List[Tuple[str, float]]] = {}
    for ts, stype, val in cur.fetchall():
        if ts is None or stype is None or val is None:
            continue
        out.setdefault(stype, []).append((normalize_ts(ts), float(val)))
    for k in out:
        out[k].sort(key=lambda x: x[0])
    return out


def fetch_external_temp_for_day_by_location(conn: sqlite3.Connection, location_id: str, day: str) -> Dict[str, float]:
    cur = conn.execute(
        "SELECT timestamp, temp_external FROM external_weather "
        "WHERE location_id = ? AND date(timestamp) = ?",
        (location_id, day),
    )
    out: Dict[str, float] = {}
    for ts, t in cur.fetchall():
        if ts is None or t is None:
            continue
        ts = normalize_ts(ts)
        out[ts[:13]] = float(t)  
    return out


def avg_in_hours(series: List[Tuple[str, float]], start_h: int, end_h: int, wrap_night: bool = False) -> Optional[float]:
    if not series:
        return None
    vals = []
    for ts, v in series:
        h = hour_of(ts)
        if wrap_night:
            if h >= start_h or h <= end_h:
                vals.append(v)
        else:
            if start_h <= h <= end_h:
                vals.append(v)
    return (sum(vals) / len(vals)) if vals else None


def compute_features(day: str, readings: Dict[str, List[Tuple[str, float]]], ext_by_hour: Dict[str, float]) -> Dict:
    occ = readings.get("occupancy", [])
    energy = readings.get("energy", [])

    avg_occ_morning = avg_in_hours(occ, MORNING_START, MORNING_END)
    avg_occ_day = avg_in_hours(occ, DAY_START, DAY_END)
    avg_occ_evening = avg_in_hours(occ, EVENING_START, EVENING_END)
    avg_occ_night = avg_in_hours(occ, NIGHT_START, NIGHT_END, wrap_night=True)

    if occ:
        binary_cnt = sum(1 for _, v in occ if (v < 0.1 or v > 0.9))
        binary_ratio = binary_cnt / len(occ)
    else:
        binary_ratio = None

    energy_vals = [v for _, v in energy]
    cons_avg = (sum(energy_vals) / len(energy_vals)) if energy_vals else None
    cons_std = std_dev(energy_vals)

    def peak_hour_between(start_h: int, end_h: int) -> Optional[int]:
        if not energy:
            return None
        buckets: Dict[int, List[float]] = {}
        for ts, v in energy:
            h = hour_of(ts)
            if start_h <= h <= end_h:
                buckets.setdefault(h, []).append(v)
        if not buckets:
            return None
        avg_by_hour = {h: (sum(vals) / len(vals)) for h, vals in buckets.items()}
        return max(avg_by_hour.items(), key=lambda x: x[1])[0]

    peak_morning = peak_hour_between(PEAK_MORNING_START, PEAK_MORNING_END)
    peak_evening = peak_hour_between(PEAK_EVENING_START, PEAK_EVENING_END)

    xs, ys = [], []
    for ts, v in energy:
        hour_key = ts[:13]
        if hour_key in ext_by_hour:
            xs.append(ext_by_hour[hour_key])
            ys.append(v)
    temp_sens = pearson_abs(xs, ys)

    dow = day_of_week(day)
    is_weekend = 1 if dow >= 5 else 0
    weekday_avg = cons_avg if (cons_avg is not None and is_weekend == 0) else None
    weekend_avg = cons_avg if (cons_avg is not None and is_weekend == 1) else None

    return {
        "date": day,
        "avg_occupancy_morning": rround(avg_occ_morning, ROUND_OCC),
        "avg_occupancy_daytime": rround(avg_occ_day, ROUND_OCC),
        "avg_occupancy_evening": rround(avg_occ_evening, ROUND_OCC),
        "avg_occupancy_nighttime": rround(avg_occ_night, ROUND_OCC),
        "binary_activity_ratio": rround(binary_ratio, ROUND_RATIO),
        "weekday_consumption_avg": rround(weekday_avg, ROUND_ENERGY),
        "weekend_consumption_avg": rround(weekend_avg, ROUND_ENERGY),
        "consumption_std_dev": rround(cons_std, ROUND_STD),
        "peak_hour_morning": peak_morning,
        "peak_hour_evening": peak_evening,
        "temp_sensitivity": rround(temp_sens, ROUND_CORR),
        "daytime_start_hour": 8,
        "daytime_end_hour": 17,
        "night_start_hour": 22,
        "night_end_hour": 6,
        "feature_version": 3,
    }


def upsert_features(conn: sqlite3.Connection, building_id: str, unit_id: str, f: Dict):
    conn.execute(
        """
        INSERT INTO unit_features_daily (
            date, building_id, unit_id,
            avg_occupancy_morning, avg_occupancy_daytime, avg_occupancy_evening, avg_occupancy_nighttime,
            binary_activity_ratio,
            weekday_consumption_avg, weekend_consumption_avg, consumption_std_dev,
            peak_hour_morning, peak_hour_evening,
            temp_sensitivity,
            daytime_start_hour, daytime_end_hour, night_start_hour, night_end_hour,
            feature_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(building_id, unit_id, date) DO UPDATE SET
            avg_occupancy_morning=excluded.avg_occupancy_morning,
            avg_occupancy_daytime=excluded.avg_occupancy_daytime,
            avg_occupancy_evening=excluded.avg_occupancy_evening,
            avg_occupancy_nighttime=excluded.avg_occupancy_nighttime,
            binary_activity_ratio=excluded.binary_activity_ratio,
            weekday_consumption_avg=excluded.weekday_consumption_avg,
            weekend_consumption_avg=excluded.weekend_consumption_avg,
            consumption_std_dev=excluded.consumption_std_dev,
            peak_hour_morning=excluded.peak_hour_morning,
            peak_hour_evening=excluded.peak_hour_evening,
            temp_sensitivity=excluded.temp_sensitivity,
            daytime_start_hour=excluded.daytime_start_hour,
            daytime_end_hour=excluded.daytime_end_hour,
            night_start_hour=excluded.night_start_hour,
            night_end_hour=excluded.night_end_hour,
            feature_version=excluded.feature_version
        """,
        (
            f["date"], building_id, unit_id,
            f["avg_occupancy_morning"], f["avg_occupancy_daytime"], f["avg_occupancy_evening"], f["avg_occupancy_nighttime"],
            f["binary_activity_ratio"],
            f["weekday_consumption_avg"], f["weekend_consumption_avg"], f["consumption_std_dev"],
            f["peak_hour_morning"], f["peak_hour_evening"],
            f["temp_sensitivity"],
            f["daytime_start_hour"], f["daytime_end_hour"], f["night_start_hour"], f["night_end_hour"],
            f["feature_version"],
        ),
    )


def run(db_path: str, building_id: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    mn, mx = fetch_days_range(conn, building_id)
    if mn is None or mx is None:
        print(f"[WARN] No sensor_readings found for building {building_id}")
        conn.close()
        return

    location_id = fetch_building_location_id(conn, building_id)
    if not location_id:
        conn.close()
        raise RuntimeError(f"Could not find location_id for building_id={building_id}.")

    units = fetch_units(conn, building_id)
    print(f"[INFO] building={building_id} location_id={location_id} units={len(units)} days={mn}..{mx}")

    d0 = datetime.strptime(mn, "%Y-%m-%d").date()
    d1 = datetime.strptime(mx, "%Y-%m-%d").date()

    cur = d0
    upserted = 0

    while cur <= d1:
        day_str = cur.strftime("%Y-%m-%d")
        ext = fetch_external_temp_for_day_by_location(conn, location_id, day_str)

        for unit_id in units:
            readings = fetch_readings_for_day(conn, building_id, unit_id, day_str)
            feats = compute_features(day_str, readings, ext)

            if (
                feats["weekday_consumption_avg"] is None
                and feats["weekend_consumption_avg"] is None
                and feats["avg_occupancy_morning"] is None
                and feats["avg_occupancy_daytime"] is None
                and feats["avg_occupancy_evening"] is None
                and feats["avg_occupancy_nighttime"] is None
            ):
                continue

            upsert_features(conn, building_id, unit_id, feats)
            upserted += 1

        conn.commit()
        cur += timedelta(days=1)

    print(f"[DONE] Upserted feature rows: {upserted}")
    conn.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--building", required=True, help="Building ID e.g. B001")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to sqlite db")
    args = p.parse_args()
    run(args.db, args.building)
