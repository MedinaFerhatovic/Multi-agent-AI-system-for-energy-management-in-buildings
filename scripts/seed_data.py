import sqlite3
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------
# CONFIG
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"

# 30 dana unazad, interval 15 min
INTERVAL_MINUTES = 15
DAYS_BACK = 30

# ---------------------------
# HELPERS
# ---------------------------
def iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def insulation_factor(level):
    return {"poor": 1.25, "average": 1.0, "good": 0.75}[level]

def ext_temp_for_time(dt, base=4.0, amp=6.0):
    hour = dt.hour + dt.minute / 60
    phase = (hour - 5) / 24 * 2 * math.pi
    return base + amp * math.sin(phase) + random.gauss(0, 0.7)

def wind_cloud_precip():
    wind = max(0, random.gauss(3.5, 1.2))
    cloud = min(100, max(0, random.gauss(65, 20)))
    precip = max(0, random.gauss(0.2, 0.5))
    return wind, cloud, precip

def tariff_price(dt):
    hour = dt.hour
    if 22 <= hour or hour < 6:
        return 0.08, "low"
    return 0.18, "high"

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
    total = sum(weights.get(p, 0.2) for p in profiles)
    r = random.random() * total
    acc = 0
    for p in profiles:
        acc += weights.get(p, 0.2)
        if r <= acc:
            return p
    return profiles[0]

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

# ---------------------------
# CORE SIMULATION
# ---------------------------
def simulate_building(conn, building_id, name, location_text, floors, units_total, building_type, insulation_level,
                      start_dt, end_dt, interval_minutes=15):

    cur = conn.cursor()

    # Insert building
    cur.execute("""
        INSERT OR REPLACE INTO buildings
        (building_id, name, location_text, floors_count, units_total, building_type, insulation_level)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (building_id, name, location_text, floors, units_total, building_type, insulation_level))

    profiles = build_profiles(building_type)

    # Create units + sensors
    units = []
    units_per_floor = max(1, math.ceil(units_total / floors))

    for i in range(1, units_total + 1):
        floor = int((i - 1) / units_per_floor) + 1
        unit_id = f"{building_id}_U{str(i).zfill(2)}"
        unit_name = f"Unit {str(i).zfill(2)} (Floor {floor})"
        area_m2 = max(25, min(140, random.gauss(55, 18)))
        profile = pick_profile(profiles)

        units.append((unit_id, unit_name, floor, area_m2, profile))

        cur.execute("""
            INSERT OR REPLACE INTO units
            (unit_id, building_id, unit_name, floor, area_m2, has_heating_control, has_cooling_control)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (unit_id, building_id, unit_name, floor, area_m2, 1, 0))

        # Sensors: always energy + temp_internal, occupancy for some units
        has_occupancy = random.random() < (0.75 if building_type != "commercial" else 0.55)

        sensor_types = ["energy", "temp_internal"]
        if has_occupancy:
            sensor_types.append("occupancy")

        for st in sensor_types:
            sensor_id = f"{unit_id}_{st}"
            cur.execute("""
                INSERT OR REPLACE INTO sensors
                (sensor_id, unit_id, sensor_type, manufacturer, model, protocol, topic_or_endpoint, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (sensor_id, unit_id, st, "sim", "v1", "mqtt",
                  f"building/{building_id}/unit/{unit_id}/{st}", 1))

    conn.commit()

    # Initialize internal temps
    t_int = {u[0]: random.gauss(20.5, 1.0) for u in units}
    ins_factor = insulation_factor(insulation_level)

    dt = start_dt
    interval_h = interval_minutes / 60.0

    readings_rows = []
    weather_rows = []
    price_rows = []

    while dt <= end_dt:
        # Weather
        t_ext = ext_temp_for_time(dt)
        wind, cloud, precip = wind_cloud_precip()
        weather_rows.append((iso(dt), building_id, t_ext, wind, cloud, precip, 0))

        # Price
        price, tariff = tariff_price(dt)
        price_rows.append((iso(dt), building_id, price, tariff, 0))

        # Per unit
        for unit_id, unit_name, floor, area_m2, profile in units:
            p_occ = occupancy_probability(profile, dt)
            occ = 1.0 if random.random() < p_occ else 0.0

            # Target temp rules
            if profile in ("res_stable", "res_variable"):
                t_target = 21.0 if occ > 0.5 else 19.0
            elif profile == "daytime_only":
                t_target = 21.0 if occ > 0.5 else 16.0
            elif profile == "continuous":
                t_target = 20.0
            else:
                t_target = 14.0

            heat_loss = (t_int[unit_id] - t_ext) * 0.06 * ins_factor * interval_h
            heat_kwh_h = heating_kwh_needed(t_int[unit_id], t_target, profile)
            heat_kwh = heat_kwh_h * interval_h
            heat_delta = (heat_kwh_h / 3.0) * 1.6 * interval_h

            t_int[unit_id] = t_int[unit_id] + heat_delta - heat_loss + random.gauss(0, 0.08)
            t_int[unit_id] = max(10.0, min(26.5, t_int[unit_id]))

            base = base_load_kwh(profile) * interval_h
            devices = devices_load_kwh(profile, occ) * interval_h
            total_kwh = base + devices + heat_kwh

            # Insert readings
            readings_rows.append((iso(dt), building_id, unit_id, "energy", total_kwh, None, "ok", "simulated"))
            readings_rows.append((iso(dt), building_id, unit_id, "temp_internal", t_int[unit_id], None, "ok", "simulated"))
            readings_rows.append((iso(dt), building_id, unit_id, "occupancy", occ, None, "ok", "simulated"))

        dt += timedelta(minutes=interval_minutes)

    cur.executemany("""
        INSERT INTO external_weather
        (timestamp, building_id, temp_external, wind_speed, cloud_cover, precipitation, forecast_hour)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, weather_rows)

    cur.executemany("""
        INSERT INTO energy_price
        (timestamp, building_id, price_per_kwh, tariff_type, forecast_hour)
        VALUES (?, ?, ?, ?, ?)
    """, price_rows)

    cur.executemany("""
        INSERT INTO sensor_readings
        (timestamp, building_id, unit_id, sensor_type, value, value2, quality_flag, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, readings_rows)

    conn.commit()


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"❌ Ne mogu pronaći bazu: {DB_PATH}")
        print("Provjeri da li smartbuilding.db postoji u db/ folderu.")
        exit(1)

    conn = sqlite3.connect(DB_PATH)

    end_dt = datetime(2026, 1, 10, 23, 45)
    start_dt = end_dt - timedelta(days=DAYS_BACK)

    # ---------------------------
    # BUILDING 1
    # ---------------------------
    simulate_building(
        conn,
        building_id="B001",
        name="Zgrada A",
        location_text="Sarajevo, Zmaja od Bosne 12",
        floors=6,
        units_total=12,
        building_type="mixed",
        insulation_level="average",
        start_dt=start_dt,
        end_dt=end_dt,
        interval_minutes=INTERVAL_MINUTES
    )

    # ---------------------------
    # BUILDING 2
    # ---------------------------
    simulate_building(
        conn,
        building_id="B002",
        name="Zgrada B",
        location_text="Sarajevo, Alipasina 45",
        floors=10,
        units_total=20,
        building_type="residential",
        insulation_level="good",
        start_dt=start_dt,
        end_dt=end_dt,
        interval_minutes=INTERVAL_MINUTES
    )

    conn.close()

    print("✅ Seed završio!")
    print("✅ Popunjene tabele: buildings, units, sensors, sensor_readings, external_weather, energy_price")
    print(f"📌 Database: {DB_PATH}")
