import sqlite3
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


# CONFIG
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"

INTERVAL_MINUTES = 30
DAYS_BACK = 20


# HELPERS
def iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def insulation_factor(level):
    return {"poor": 1.25, "average": 1.0, "good": 0.75}.get(level, 1.0)

def ext_temp_for_time(dt, base=4.0, amp=6.0):
    hour = dt.hour + dt.minute / 60
    phase = (hour - 5) / 24 * 2 * math.pi
    temp = base + amp * math.sin(phase) + random.gauss(0, 0.7)
    return round(temp, 1)

def wind_cloud_precip():
    wind_ms = max(0, random.gauss(3.5, 1.2))
    wind_kmh = round(wind_ms * 3.6, 1)  # km/h

    cloud = int(min(100, max(0, random.gauss(65, 20))))  # %
    precip = round(max(0, random.gauss(0.2, 0.5)), 2)    # mm

    return wind_kmh, cloud, precip

def build_profiles(building_type):
    if building_type == "residential":
        return ["res_stable", "res_variable", "vacant"]
    if building_type == "commercial":
        return ["daytime_only", "continuous", "vacant"]
    return ["res_stable", "res_variable", "daytime_only", "vacant", "continuous"]

def pick_profile(profiles):
    weights = {
        "res_stable": 0.35,
        "res_variable": 0.25,
        "daytime_only": 0.2,
        "vacant": 0.1,
        "continuous": 0.1
    }
    return random.choices(profiles, weights=[weights.get(p, 0.2) for p in profiles], k=1)[0]

def generate_unit_numbers(floors, units_total):
    unit_numbers = []
    units_per_floor = math.ceil(units_total / floors)

    counter = 1
    for floor in range(1, floors + 1):
        for i in range(1, units_per_floor + 1):
            if counter > units_total:
                break
            unit_numbers.append(str(floor * 100 + i))
            counter += 1
    return unit_numbers

def sample_area_from_distribution(dist):
    categories = ["small", "medium", "large"]
    weights = [dist["small_pct"], dist["medium_pct"], dist["large_pct"]]
    choice = random.choices(categories, weights=weights, k=1)[0]

    if choice == "small":
        return int(round(max(20, random.gauss(dist["small_avg"], 4))))
    if choice == "medium":
        return int(round(max(30, random.gauss(dist["medium_avg"], 6))))
    return int(round(max(45, random.gauss(dist["large_avg"], 10))))


# OCCUPANCY + LOADS
def occupancy_probability(profile, dt):
    dow = dt.weekday()
    hour = dt.hour + dt.minute / 60
    weekend = dow >= 5

    if profile == "vacant":
        return 0.02 if (9 <= hour <= 18 and random.random() < 0.05) else 0.0

    if profile == "daytime_only":
        if weekend:
            return 0.02
        if 8 <= hour < 9:
            return 0.3 + 0.6 * (hour - 8)
        if 9 <= hour < 17:
            return 0.9
        if 17 <= hour < 18:
            return 0.9 - 0.8 * (hour - 17)
        return 0.01

    if profile == "continuous":
        return 0.7 if not weekend else 0.8

    if profile == "res_stable":
        if 0 <= hour < 6:
            return 0.95
        if 6 <= hour < 9:
            return 0.75
        if 9 <= hour < 15:
            return 0.45 if not weekend else 0.7
        if 15 <= hour < 22:
            return 0.85
        return 0.95

    if profile == "res_variable":
        base = 0.55 if weekend else 0.4
        spike = 0.25 * math.exp(-((hour - 21) / 2.5) ** 2) + 0.2 * math.exp(-((hour - 1) / 2.0) ** 2)
        noise = random.gauss(0, 0.15)
        return min(0.98, max(0.02, base + spike + noise))

    return 0.2

def base_load_kwh(profile):
    if profile == "daytime_only":
        return 0.15
    if profile == "continuous":
        return 0.4
    if profile == "vacant":
        return 0.08
    return 0.18

def devices_load_kwh(profile, occ):
    if occ < 0.5:
        return 0.0 if profile in ("vacant", "daytime_only") else max(0, random.gauss(0.05, 0.05))

    if profile == "daytime_only":
        return max(0.3, random.gauss(0.8, 0.25))
    if profile == "continuous":
        return max(0.2, random.gauss(0.6, 0.2))
    if profile == "res_stable":
        return max(0.2, random.gauss(0.5, 0.25))
    if profile == "res_variable":
        return max(0.15, random.gauss(0.6, 0.35))

    return max(0.1, random.gauss(0.4, 0.2))

def heating_kwh_needed(t_internal, t_target, profile):
    if profile == "vacant":
        t_target = min(t_target, 15.0)
    if t_internal < t_target - 0.2:
        gap = t_target - t_internal
        duty = min(1.0, gap / 3.0)
        return 3.0 * duty
    return 0.0

def humidity_for_time(dt, occ):
    base = 45 + (10 if occ > 0.5 else 0)
    daily_wave = 5 * math.sin((dt.hour / 24) * 2 * math.pi)
    hum = base + daily_wave + random.gauss(0, 4)
    return round(min(80, max(25, hum)), 1)

# LOCATION + WEATHER SEED
def seed_locations(conn):
    cur = conn.cursor()

    locations = [
        ("LOC_SA", "Sarajevo, Zmaja od Bosne 12", "Sarajevo", 43.8563, 18.4131),
        ("LOC_ZE", "Zenica, Bulevar Kralja Tvrtka 1", "Zenica", 44.2034, 17.9070),
    ]

    cur.executemany("""
        INSERT OR REPLACE INTO locations
        (location_id, location_text, city, lat, lon)
        VALUES (?, ?, ?, ?, ?)
    """, locations)

    conn.commit()

def seed_weather_for_location(conn, location_id, start_dt, end_dt):
    cur = conn.cursor()
    dt = start_dt
    rows = []

    while dt <= end_dt:
        t_ext = ext_temp_for_time(dt)
        wind_kmh, cloud_pct, precip_mm = wind_cloud_precip()

        rows.append((iso(dt), location_id, t_ext, wind_kmh, cloud_pct, precip_mm, 0))
        dt += timedelta(minutes=INTERVAL_MINUTES)

    cur.executemany("""
        INSERT INTO external_weather
        (timestamp, location_id, temp_external, wind_speed_kmh, cloud_cover, precipitation_mm, forecast_hour)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()

# BUILDING SEED (tariff_model + units + sensors + sensor_readings)
def simulate_building(conn, building_id, name, location_id, floors, units_total,
                      building_type, insulation_level, area_distribution,
                      start_dt, end_dt, interval_minutes=15):

    cur = conn.cursor()

    location_text = cur.execute(
        "SELECT location_text FROM locations WHERE location_id=?",
        (location_id,)
    ).fetchone()[0]

    cur.execute("""
        INSERT OR REPLACE INTO buildings
        (building_id, name, location_text, floors_count, units_total, building_type, insulation_level, location_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (building_id, name, location_text, floors, units_total, building_type, insulation_level, location_id))

    cur.execute("""
        INSERT OR REPLACE INTO tariff_model
        (building_id, low_tariff_start, low_tariff_end, low_price_per_kwh, high_price_per_kwh, sunday_all_day_low, currency)
        VALUES (?, '22:00', '06:00', 0.08, 0.18, 1, 'BAM')
    """, (building_id,))

    profiles = build_profiles(building_type)
    unit_numbers = generate_unit_numbers(floors, units_total)

    units = []
    for unit_number in unit_numbers:
        floor = int(unit_number) // 100
        unit_id = f"{building_id}_U{unit_number}"

        area_initial = sample_area_from_distribution(area_distribution)
        profile = pick_profile(profiles)

        units.append((unit_id, unit_number, floor, area_initial, profile))

        cur.execute("""
            INSERT OR REPLACE INTO units
            (unit_id, building_id, unit_number, floor,
             area_m2_initial, area_m2_estimated, area_m2_final,
             area_source, area_confidence,
             has_heating_control, has_cooling_control)
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, 0)
        """, (
            unit_id, building_id, unit_number, floor,
            area_initial, area_initial,
            "user_avg_distribution", 0.0
        ))

        has_occupancy = random.random() < (0.75 if building_type != "commercial" else 0.55)
        sensor_types = ["energy", "temp_internal", "humidity"]
        if has_occupancy:
            sensor_types.append("occupancy")

        for st in sensor_types:
            sensor_id = f"{unit_id}_{st}"
            cur.execute("""
                INSERT OR REPLACE INTO sensors
                (sensor_id, unit_id, sensor_type, manufacturer, model, protocol, topic_or_endpoint, active)
                VALUES (?, ?, ?, 'sim', 'v1', 'mqtt', ?, 1)
            """, (
                sensor_id, unit_id, st,
                f"building/{building_id}/unit/{unit_number}/{st}"
            ))

    conn.commit()

    t_int = {u[0]: random.gauss(20.5, 1.0) for u in units}
    ins_factor = insulation_factor(insulation_level)

    dt = start_dt
    interval_h = interval_minutes / 60.0

    readings_rows = []

    occ_sensor_units = set(
        r[0] for r in cur.execute("SELECT unit_id FROM sensors WHERE sensor_type='occupancy'").fetchall()
    )

    while dt <= end_dt:

        t_ext_row = cur.execute("""
            SELECT temp_external
            FROM external_weather
            WHERE location_id=? AND timestamp=?
            LIMIT 1
        """, (location_id, iso(dt))).fetchone()

        t_ext = t_ext_row[0] if t_ext_row else ext_temp_for_time(dt)

        for unit_id, unit_number, floor, area_initial, profile in units:
            p_occ = occupancy_probability(profile, dt)
            occ = 1.0 if random.random() < p_occ else 0.0

            humidity = humidity_for_time(dt, occ)

            if profile in ("res_stable", "res_variable"):
                t_target = 21.0 if occ > 0.5 else 19.0
            elif profile == "daytime_only":
                t_target = 21.0 if occ > 0.5 else 16.0
            elif profile == "continuous":
                t_target = 20.0
            else:
                t_target = 14.0

            area_factor = min(1.4, max(0.7, area_initial / 60.0))
            heat_loss = (t_int[unit_id] - t_ext) * 0.06 * ins_factor * area_factor * interval_h

            heat_kwh_h = heating_kwh_needed(t_int[unit_id], t_target, profile)
            heat_kwh = heat_kwh_h * interval_h
            heat_delta = (heat_kwh_h / 3.0) * 1.6 * interval_h

            t_int[unit_id] = t_int[unit_id] + heat_delta - heat_loss + random.gauss(0, 0.08)
            t_int[unit_id] = max(10.0, min(26.5, t_int[unit_id]))

            base = base_load_kwh(profile) * interval_h
            devices = devices_load_kwh(profile, occ) * interval_h
            total_kwh = base + devices + heat_kwh

            readings_rows.append((iso(dt), building_id, unit_id, "energy", round(total_kwh, 3), None, "ok", "simulated"))
            readings_rows.append((iso(dt), building_id, unit_id, "temp_internal", round(t_int[unit_id], 1), None, "ok", "simulated"))
            readings_rows.append((iso(dt), building_id, unit_id, "humidity", humidity, None, "ok", "simulated"))

            if unit_id in occ_sensor_units:
                readings_rows.append((iso(dt), building_id, unit_id, "occupancy", occ, None, "ok", "simulated"))

        dt += timedelta(minutes=interval_minutes)

    cur.executemany("""
        INSERT INTO sensor_readings
        (timestamp, building_id, unit_id, sensor_type, value, value2, quality_flag, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, readings_rows)

    conn.commit()

# MAIN
if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"No database: {DB_PATH}")
        exit(1)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    end_dt = datetime(2026, 1, 10, 23, 45)
    start_dt = end_dt - timedelta(days=DAYS_BACK)

    seed_locations(conn)

    seed_weather_for_location(conn, "LOC_SA", start_dt, end_dt)
    seed_weather_for_location(conn, "LOC_ZE", start_dt, end_dt)

    building1_area_dist = {
        "small_avg": 35, "medium_avg": 55, "large_avg": 85,
        "small_pct": 40, "medium_pct": 45, "large_pct": 15
    }

    building2_area_dist = {
        "small_avg": 30, "medium_avg": 50, "large_avg": 80,
        "small_pct": 50, "medium_pct": 35, "large_pct": 15
    }

    # simulate buildings (includes tariff_model + units + sensors + sensor_readings)
    simulate_building(
        conn,
        building_id="B001",
        name="Zgrada Sarajevo",
        location_id="LOC_SA",
        floors=6,
        units_total=12,
        building_type="mixed",
        insulation_level="average",
        area_distribution=building1_area_dist,
        start_dt=start_dt,
        end_dt=end_dt,
        interval_minutes=INTERVAL_MINUTES
    )

    simulate_building(
        conn,
        building_id="B002",
        name="Zgrada Zenica",
        location_id="LOC_ZE",
        floors=10,
        units_total=20,
        building_type="residential",
        insulation_level="good",
        area_distribution=building2_area_dist,
        start_dt=start_dt,
        end_dt=end_dt,
        interval_minutes=INTERVAL_MINUTES
    )

    conn.close()

    print("Finished!")
    print("Tables: locations, buildings, tariff_model, units, sensors, external_weather, sensor_readings")
    print(f"Database: {DB_PATH}")
