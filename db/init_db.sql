PRAGMA foreign_keys = ON;

-- =========================
-- 1) BUILDINGS
-- =========================
CREATE TABLE IF NOT EXISTS buildings (
    building_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location_text TEXT NOT NULL,
    floors_count INTEGER NOT NULL,
    units_total INTEGER NOT NULL,
    building_type TEXT NOT NULL CHECK(building_type IN ('residential', 'commercial', 'mixed')),
    insulation_level TEXT NOT NULL CHECK(insulation_level IN ('poor', 'average', 'good')),
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- 2) UNITS (zones)
-- =========================
CREATE TABLE IF NOT EXISTS units (
    unit_id TEXT PRIMARY KEY,
    building_id TEXT NOT NULL,
    unit_name TEXT NOT NULL,
    floor INTEGER,
    area_m2 REAL,
    has_heating_control INTEGER DEFAULT 1,
    has_cooling_control INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

-- =========================
-- 3) SENSORS
-- =========================
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    manufacturer TEXT,
    model TEXT,
    protocol TEXT,
    topic_or_endpoint TEXT,
    active INTEGER DEFAULT 1,
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

-- =========================
-- 4) SENSOR_READINGS (main timeseries)
-- =========================
CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    value REAL NOT NULL,
    value2 REAL,
    quality_flag TEXT DEFAULT 'ok',
    source TEXT DEFAULT 'simulated',
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

CREATE INDEX IF NOT EXISTS idx_readings_time ON sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_readings_unit_time ON sensor_readings(unit_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_readings_building_time ON sensor_readings(building_id, timestamp);

-- =========================
-- 5) EXTERNAL WEATHER
-- =========================
CREATE TABLE IF NOT EXISTS external_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    temp_external REAL NOT NULL,
    wind_speed REAL,
    cloud_cover REAL,
    precipitation REAL,
    forecast_hour INTEGER DEFAULT 0,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_time ON external_weather(timestamp);

-- =========================
-- 6) ENERGY PRICE
-- =========================
CREATE TABLE IF NOT EXISTS energy_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    price_per_kwh REAL NOT NULL,
    tariff_type TEXT NOT NULL,
    forecast_hour INTEGER DEFAULT 0,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

CREATE INDEX IF NOT EXISTS idx_price_time ON energy_price(timestamp);

-- =========================
-- 7) UNIT_FEATURES_DAILY
-- =========================
CREATE TABLE IF NOT EXISTS unit_features_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    avg_occupancy_daytime REAL,
    avg_occupancy_nighttime REAL,
    binary_activity_ratio REAL,
    weekday_consumption_avg REAL,
    weekend_consumption_avg REAL,
    consumption_std_dev REAL,
    peak_hour_morning INTEGER,
    peak_hour_evening INTEGER,
    temp_sensitivity REAL,
    window_open_freq REAL,
    feature_version INTEGER DEFAULT 1,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

CREATE INDEX IF NOT EXISTS idx_features_unit_date ON unit_features_daily(unit_id, date);

-- =========================
-- 8) CLUSTERS
-- =========================
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id TEXT PRIMARY KEY,
    building_id TEXT NOT NULL,
    cluster_name TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

-- =========================
-- 9) UNIT_CLUSTER_ASSIGNMENT
-- =========================
CREATE TABLE IF NOT EXISTS unit_cluster_assignment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT,
    confidence REAL DEFAULT 0.0,
    reason TEXT,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id),
    FOREIGN KEY(cluster_id) REFERENCES clusters(cluster_id)
);

-- =========================
-- 10) PREDICTIONS
-- =========================
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_created TEXT NOT NULL,
    timestamp_target TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    predicted_consumption REAL,
    predicted_occupancy_prob REAL,
    model_name TEXT,
    confidence REAL,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

-- =========================
-- 11) OPTIMIZATION_PLANS
-- =========================
CREATE TABLE IF NOT EXISTS optimization_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    target_temp REAL,
    start_time TEXT,
    end_time TEXT,
    estimated_cost REAL,
    estimated_savings REAL,
    method TEXT,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

-- =========================
-- 12) DECISIONS_LOG
-- =========================
CREATE TABLE IF NOT EXISTS decisions_log (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    action TEXT NOT NULL,
    approved INTEGER DEFAULT 1,
    reasoning_text TEXT,
    confidence REAL,
    mode TEXT DEFAULT 'learning',
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

-- =========================
-- 13) ANOMALIES_LOG
-- =========================
CREATE TABLE IF NOT EXISTS anomalies_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    sensor_id TEXT,
    anomaly_type TEXT NOT NULL,
    value REAL,
    severity TEXT DEFAULT 'medium',
    action_taken TEXT,
    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id),
    FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id)
);
