# scripts/train_models.py
import sqlite3
import numpy as np
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"
MODELS_DIR = BASE_DIR / "models"

# Kreiraj folder ako ne postoji
MODELS_DIR.mkdir(exist_ok=True)


def fetch_training_data(conn, building_id, lookback_days=7):
    """
    Извлачи података за тренирање модела.
    Features: historical consumption, occupancy, weather, temporal features
    Target: next consumption
    """
    query = """
    SELECT 
        sr.timestamp,
        sr.unit_id,
        sr.value as current_consumption,
        u.area_m2_final,
        ew.temp_external,
        ew.wind_speed_kmh,
        ew.cloud_cover
    FROM sensor_readings sr
    JOIN units u ON sr.unit_id = u.unit_id
    LEFT JOIN buildings b ON sr.building_id = b.building_id
    LEFT JOIN external_weather ew ON b.location_id = ew.location_id 
        AND datetime(sr.timestamp) = datetime(ew.timestamp)
    WHERE sr.building_id = ? 
        AND sr.sensor_type = 'energy'
        AND sr.quality_flag = 'ok'
    ORDER BY sr.unit_id, sr.timestamp
    """
    
    cursor = conn.execute(query, (building_id,))
    rows = cursor.fetchall()
    
    if not rows:
        return None, None, None
    
    # Organizuj po unit_id
    unit_data = {}
    for row in rows:
        ts, unit_id, consumption, area, temp_ext, wind, cloud = row
        
        if unit_id not in unit_data:
            unit_data[unit_id] = []
        
        unit_data[unit_id].append({
            'timestamp': ts,
            'consumption': consumption,
            'area': area if area else 50.0,
            'temp_external': temp_ext if temp_ext else 0.0,
            'wind_speed': wind if wind else 0.0,
            'cloud_cover': cloud if cloud else 0
        })
    
    return unit_data


def create_sequences(unit_data, sequence_length=48):
    """
    Kreira sekvence za prediction (sliding window).
    sequence_length = broj prethodnih readings (48 = 24h sa 30min interval)
    """
    X, y, unit_ids = [], [], []
    
    for unit_id, records in unit_data.items():
        if len(records) < sequence_length + 1:
            continue
        
        for i in range(len(records) - sequence_length):
            # Features: last sequence_length readings
            sequence = records[i:i + sequence_length]
            target = records[i + sequence_length]
            
            # Extract features
            consumptions = [r['consumption'] for r in sequence]
            temps = [r['temp_external'] for r in sequence]
            
            # Temporal features from target timestamp
            dt = datetime.fromisoformat(target['timestamp'].replace('Z', '+00:00'))
            hour = dt.hour
            day_of_week = dt.weekday()
            is_weekend = 1 if day_of_week >= 5 else 0
            
            features = [
                # Historical consumption stats
                np.mean(consumptions),
                np.std(consumptions),
                np.max(consumptions),
                np.min(consumptions),
                consumptions[-1],  # last value
                
                # Weather features
                np.mean(temps),
                target['temp_external'],
                target['wind_speed'],
                target['cloud_cover'],
                
                # Unit features
                target['area'],
                
                # Temporal features
                hour,
                day_of_week,
                is_weekend,
                np.sin(2 * np.pi * hour / 24),  # cyclical hour
                np.cos(2 * np.pi * hour / 24),
            ]
            
            X.append(features)
            y.append(target['consumption'])
            unit_ids.append(unit_id)
    
    return np.array(X), np.array(y), unit_ids


def train_consumption_model(X, y, model_type='random_forest'):
    """Trenira model za predviđanje potrošnje"""
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Normalize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model
    print(f"🤖 Training {model_type} model...")
    
    if model_type == 'random_forest':
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            random_state=42,
            n_jobs=-1
        )
    else:  # gradient_boosting
        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
    
    model.fit(X_train_scaled, y_train)
    
    # Evaluate
    y_pred_train = model.predict(X_train_scaled)
    y_pred_test = model.predict(X_test_scaled)
    
    metrics = {
        'train_mse': mean_squared_error(y_train, y_pred_train),
        'test_mse': mean_squared_error(y_test, y_pred_test),
        'train_mae': mean_absolute_error(y_train, y_pred_train),
        'test_mae': mean_absolute_error(y_test, y_pred_test),
        'train_r2': r2_score(y_train, y_pred_train),
        'test_r2': r2_score(y_test, y_pred_test)
    }
    
    return model, scaler, metrics


def save_model(model, scaler, building_id, model_name, metrics):
    """Čuva model i scaler"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    model_data = {
        'model': model,
        'scaler': scaler,
        'building_id': building_id,
        'trained_at': timestamp,
        'metrics': metrics,
        'model_type': model_name
    }
    
    filename = f"{building_id}_{model_name}_{timestamp}.pkl"
    filepath = MODELS_DIR / filename
    
    with open(filepath, 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"💾 Model saved: {filepath}")
    return filepath


def train_for_building(conn, building_id, model_type='random_forest'):
    """Main training pipeline"""
    print(f"\n{'='*60}")
    print(f"🚀 TRAINING MODEL for building: {building_id}")
    print(f"{'='*60}")
    
    # 1. Fetch data
    print("📊 Fetching training data...")
    unit_data = fetch_training_data(conn, building_id)
    
    if not unit_data:
        print("[ERROR] No training data available")
        return
    
    print(f"✅ Loaded data for {len(unit_data)} units")
    
    # 2. Create sequences
    print("🔄 Creating feature sequences...")
    X, y, unit_ids = create_sequences(unit_data)
    
    if len(X) == 0:
        print("[ERROR] Not enough data to create sequences")
        return
    
    print(f"✅ Created {len(X)} training samples")
    print(f"   Feature shape: {X.shape}")
    print(f"   Target shape: {y.shape}")
    
    # 3. Train model
    model, scaler, metrics = train_consumption_model(X, y, model_type)
    
    # 4. Print metrics
    print(f"\n{'='*60}")
    print("📈 MODEL PERFORMANCE")
    print(f"{'='*60}")
    print(f"Train MSE: {metrics['train_mse']:.4f}")
    print(f"Test MSE:  {metrics['test_mse']:.4f}")
    print(f"Train MAE: {metrics['train_mae']:.4f}")
    print(f"Test MAE:  {metrics['test_mae']:.4f}")
    print(f"Train R²:  {metrics['train_r2']:.4f}")
    print(f"Test R²:   {metrics['test_r2']:.4f}")
    
    # 5. Save model
    model_path = save_model(model, scaler, building_id, model_type, metrics)
    
    print(f"\n{'='*60}")
    print("✅ Training completed successfully!")
    print(f"{'='*60}\n")
    
    return model_path


def run(db_path, building_id, model_type='random_forest'):
    """Entry point"""
    conn = sqlite3.connect(db_path)
    try:
        train_for_building(conn, building_id, model_type)
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Train consumption prediction models")
    parser.add_argument("--building", required=True, help="Building ID (e.g., B001)")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to database")
    parser.add_argument("--model", choices=['random_forest', 'gradient_boosting'], 
                       default='random_forest', help="Model type")
    
    args = parser.parse_args()
    run(args.db, args.building, args.model)