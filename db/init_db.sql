PRAGMA foreign_keys = ON;

-- 0) LOCATIONS
CREATE TABLE IF NOT EXISTS locations (
    location_id TEXT PRIMARY KEY,
    location_text TEXT NOT NULL,
    city TEXT NOT NULL,
    lat REAL,
    lon REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_unique
ON locations(location_text);

-- 1) BUILDINGS
CREATE TABLE IF NOT EXISTS buildings (
    building_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,

    location_text TEXT NOT NULL,

    location_id TEXT NOT NULL,

    floors_count INTEGER NOT NULL,
    units_total INTEGER NOT NULL,

    building_type TEXT DEFAULT 'mixed',
    insulation_level TEXT DEFAULT 'average', -- poor / average / good

    created_at TEXT DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY(location_id) REFERENCES locations(location_id)
);

CREATE INDEX IF NOT EXISTS idx_buildings_location
ON buildings(location_id);


-- 2) TARIFF MODEL
CREATE TABLE IF NOT EXISTS tariff_model (
    building_id TEXT PRIMARY KEY,

    low_tariff_start TEXT NOT NULL DEFAULT '22:00',
    low_tariff_end   TEXT NOT NULL DEFAULT '06:00',

    low_price_per_kwh  REAL NOT NULL DEFAULT 0.08,
    high_price_per_kwh REAL NOT NULL DEFAULT 0.18,

    sunday_all_day_low INTEGER NOT NULL DEFAULT 1,

    currency TEXT DEFAULT 'BAM',

    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

-- 3) UNITS 
CREATE TABLE IF NOT EXISTS units (
    unit_id TEXT PRIMARY KEY,
    building_id TEXT NOT NULL,

    unit_number TEXT NOT NULL,  
    floor INTEGER NOT NULL,

    area_m2_initial INTEGER,
    area_m2_estimated INTEGER,
    area_m2_final INTEGER,

    area_source TEXT DEFAULT 'generated_distribution',
    area_confidence REAL DEFAULT 0.0
        CHECK(area_confidence >= 0.0 AND area_confidence <= 1.0),

    has_heating_control INTEGER DEFAULT 1,
    has_cooling_control INTEGER DEFAULT 0,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY(building_id) REFERENCES buildings(building_id),

    UNIQUE(building_id, unit_number),
    CHECK(area_m2_final IS NULL OR area_m2_final >= 0)
);

CREATE INDEX IF NOT EXISTS idx_units_building
ON units(building_id);

-- 4) SENSORS
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL,

    sensor_type TEXT NOT NULL,  -- energy, temp_internal, humidity, occupancy...

    manufacturer TEXT,
    model TEXT,
    protocol TEXT,
    topic_or_endpoint TEXT,
    active INTEGER DEFAULT 1,

    FOREIGN KEY(unit_id) REFERENCES units(unit_id)
);

CREATE INDEX IF NOT EXISTS idx_sensors_unit
ON sensors(unit_id);

-- 5) SENSOR_READINGS (main timeseries)
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

CREATE INDEX IF NOT EXISTS idx_readings_time
ON sensor_readings(timestamp);

CREATE INDEX IF NOT EXISTS idx_readings_unit_time
ON sensor_readings(unit_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_readings_building_time
ON sensor_readings(building_id, timestamp);


-- 6) EXTERNAL WEATHER (linked to location)
CREATE TABLE IF NOT EXISTS external_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,

    location_id TEXT NOT NULL,

    temp_external REAL NOT NULL,       -- °C 
    wind_speed_kmh REAL NOT NULL,      -- km/h 
    cloud_cover INTEGER NOT NULL,      -- % 
    precipitation_mm REAL NOT NULL,    -- mm 

    forecast_hour INTEGER DEFAULT 0,   -- 0=current, >0 forecast

    FOREIGN KEY(location_id) REFERENCES locations(location_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_time
ON external_weather(timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_location_time
ON external_weather(location_id, timestamp);


-- 7) UNIT_FEATURES_DAILY
CREATE TABLE IF NOT EXISTS unit_features_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    date TEXT NOT NULL,                 
    building_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,

    avg_occupancy_morning REAL,         -- 06:00-07:59
    avg_occupancy_daytime REAL,         -- 08:00-16:59
    avg_occupancy_evening REAL,         -- 17:00-21:59
    avg_occupancy_nighttime REAL,       -- 22:00-05:59

    binary_activity_ratio REAL,         -- 0..1 (dimensionless)

    -- Energy features (same unit as sensor_readings.value for sensor_type='energy')
    weekday_consumption_avg REAL,       -- avg energy for that date if weekday else NULL
    weekend_consumption_avg REAL,       -- avg energy for that date if weekend else NULL
    consumption_std_dev REAL,           -- std dev energy for that date

    -- Peak hour (0..23)
    peak_hour_morning INTEGER,          -- peak in 06..12
    peak_hour_evening INTEGER,          -- peak in 16..22

    -- External temperature sensitivity 
    temp_sensitivity REAL,

    -- Audit / definition 
    daytime_start_hour INTEGER NOT NULL DEFAULT 8,
    daytime_end_hour   INTEGER NOT NULL DEFAULT 17,
    night_start_hour   INTEGER NOT NULL DEFAULT 22,
    night_end_hour     INTEGER NOT NULL DEFAULT 6,

    feature_version INTEGER NOT NULL DEFAULT 3,

    FOREIGN KEY(building_id) REFERENCES buildings(building_id),
    FOREIGN KEY(unit_id) REFERENCES units(unit_id),

    UNIQUE(building_id, unit_id, date)
);

CREATE INDEX IF NOT EXISTS idx_features_unit_date
ON unit_features_daily(unit_id, date);

CREATE INDEX IF NOT EXISTS idx_features_building_date
ON unit_features_daily(building_id, date);


-- 8) CLUSTERS
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id TEXT PRIMARY KEY,
    building_id TEXT NOT NULL,
    cluster_name TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_updated TEXT DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY(building_id) REFERENCES buildings(building_id)
);

-- 9) UNIT_CLUSTER_ASSIGNMENT
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


-- 10) PREDICTIONS
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

-- 11) OPTIMIZATION_PLANS
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


-- 12) DECISIONS_LOG
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


-- 13) ANOMALIES_LOG
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

-- 14) MODEL_REGISTRY
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
);

CREATE INDEX IF NOT EXISTS idx_model_registry_active
ON model_registry(is_active, model_task);

CREATE INDEX IF NOT EXISTS idx_model_registry_scope
ON model_registry(model_scope, model_task);

-- 15) PIPELINE_PROGRESS
CREATE TABLE IF NOT EXISTS pipeline_progress (
  pipeline_name TEXT NOT NULL,
  building_id   TEXT NOT NULL,
  current_anchor_ts TEXT NOT NULL,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (pipeline_name, building_id)
);

-- 16) SYSTEM_VALIDATION_LOG
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
);

CREATE INDEX IF NOT EXISTS idx_decisions_building_time
ON decisions_log(building_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_severity
ON anomalies_log(building_id, unit_id, severity, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_predictions_target
ON predictions(building_id, timestamp_target DESC);

CREATE INDEX IF NOT EXISTS idx_optim_plans_building_time
ON optimization_plans(building_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_sr_energy_composite
ON sensor_readings(sensor_type, quality_flag, building_id, unit_id, timestamp)
WHERE sensor_type = 'energy' AND quality_flag = 'ok';

CREATE INDEX IF NOT EXISTS idx_units_building_floor
ON units(building_id, floor);

CREATE INDEX IF NOT EXISTS idx_validation_status
ON system_validation_log(building_id, status, timestamp DESC);
