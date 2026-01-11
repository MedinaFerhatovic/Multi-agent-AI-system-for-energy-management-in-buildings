# scripts/clustering.py
import sqlite3
import numpy as np
from datetime import datetime
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"

def fetch_features_for_clustering(conn, building_id):
    """Извлачи фичове за кластеровање"""
    query = """
    SELECT 
        unit_id,
        AVG(avg_occupancy_morning) as avg_occ_morning,
        AVG(avg_occupancy_daytime) as avg_occ_day,
        AVG(avg_occupancy_evening) as avg_occ_evening,
        AVG(avg_occupancy_nighttime) as avg_occ_night,
        AVG(binary_activity_ratio) as avg_binary_ratio,
        AVG(COALESCE(weekday_consumption_avg, weekend_consumption_avg)) as avg_consumption,
        AVG(consumption_std_dev) as avg_std_dev,
        AVG(temp_sensitivity) as avg_temp_sens
    FROM unit_features_daily
    WHERE building_id = ?
    GROUP BY unit_id
    HAVING COUNT(*) >= 5  -- barem 5 dana podataka
    """
    
    cursor = conn.execute(query, (building_id,))
    rows = cursor.fetchall()
    
    if not rows:
        print(f"[WARN] No features found for building {building_id}")
        return None, None
    
    unit_ids = [r[0] for r in rows]
    features = []
    
    for r in rows:
        # Ako su NULL vrijednosti, zamijeni sa 0
        feat = [
            r[1] if r[1] is not None else 0.0,  # avg_occ_morning
            r[2] if r[2] is not None else 0.0,  # avg_occ_day
            r[3] if r[3] is not None else 0.0,  # avg_occ_evening
            r[4] if r[4] is not None else 0.0,  # avg_occ_night
            r[5] if r[5] is not None else 0.0,  # avg_binary_ratio
            r[6] if r[6] is not None else 0.0,  # avg_consumption
            r[7] if r[7] is not None else 0.0,  # avg_std_dev
            r[8] if r[8] is not None else 0.0,  # avg_temp_sens
        ]
        features.append(feat)
    
    return unit_ids, np.array(features)


def determine_optimal_clusters(X, max_k=6):
    """Određuje optimalan broj klastera koristeći Elbow metodu"""
    if len(X) < 3:
        return 2
    
    max_k = min(max_k, len(X) - 1)
    inertias = []
    
    for k in range(2, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X)
        inertias.append(kmeans.inertia_)
    
    # Pronađi "lakat" - najveću razliku
    diffs = np.diff(inertias)
    optimal_k = np.argmin(diffs) + 2
    
    return optimal_k


def perform_clustering(conn, building_id, n_clusters=None):
    """Glavni clustering proces"""
    print(f"\n{'='*60}")
    print(f"🔍 CLUSTERING for building: {building_id}")
    print(f"{'='*60}")
    
    # 1. Fetch features
    unit_ids, features = fetch_features_for_clustering(conn, building_id)
    
    if features is None or len(features) < 3:
        print("[ERROR] Not enough data for clustering (need at least 3 units)")
        return
    
    print(f"✅ Loaded {len(unit_ids)} units with features")
    
    # 2. Normalizuj features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # 3. Odredi broj klastera ako nije specificiran
    if n_clusters is None:
        n_clusters = determine_optimal_clusters(features_scaled)
    
    print(f"📊 Using {n_clusters} clusters")
    
    # 4. Primijeni KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(features_scaled)
    
    # 5. PCA za 2D vizualizaciju (opciono, za provjeru)
    pca = PCA(n_components=2)
    features_2d = pca.fit_transform(features_scaled)
    
    print(f"📈 PCA explained variance: {pca.explained_variance_ratio_.sum():.2%}")
    
    # 6. Kreiraj cluster imena
    cluster_names = {
        0: "High Activity",
        1: "Medium Activity", 
        2: "Low Activity",
        3: "Variable Pattern",
        4: "Vacant/Minimal",
        5: "Commercial Hours"
    }
    
    # 7. Pohrani rezultate u bazu
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    # Prvo obriši stare clustere za ovo zdanje
    conn.execute("DELETE FROM clusters WHERE building_id = ?", (building_id,))
    conn.execute("DELETE FROM unit_cluster_assignment WHERE building_id = ?", (building_id,))
    
    # Kreiraj nove clustere
    for cluster_id in range(n_clusters):
        cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
        
        conn.execute("""
            INSERT INTO clusters (cluster_id, building_id, cluster_name, created_at, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (
            f"{building_id}_C{cluster_id}",
            building_id,
            cluster_name,
            timestamp,
            timestamp
        ))
    
    # Assign units to clusters
    for unit_id, cluster_label in zip(unit_ids, labels):
        cluster_id = f"{building_id}_C{cluster_label}"
        
        # Izračunaj confidence (distance to centroid)
        unit_idx = unit_ids.index(unit_id)
        distance = np.linalg.norm(features_scaled[unit_idx] - kmeans.cluster_centers_[cluster_label])
        confidence = max(0.0, 1.0 - (distance / 3.0))  # normalize to 0-1
        
        conn.execute("""
            INSERT INTO unit_cluster_assignment 
            (building_id, unit_id, cluster_id, start_date, confidence, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            building_id,
            unit_id,
            cluster_id,
            datetime.now().date().isoformat(),
            round(confidence, 3),
            "kmeans_clustering"
        ))
    
    conn.commit()
    
    # 8. Prikaži rezultate
    print(f"\n{'='*60}")
    print("📋 CLUSTERING RESULTS")
    print(f"{'='*60}")
    
    for cluster_id in range(n_clusters):
        cluster_units = [u for u, l in zip(unit_ids, labels) if l == cluster_id]
        cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
        
        print(f"\n🏷️  {cluster_name} ({len(cluster_units)} units)")
        print(f"   Units: {', '.join([u.split('_')[-1] for u in cluster_units[:5]])}", end="")
        if len(cluster_units) > 5:
            print(f" ... (+{len(cluster_units)-5} more)")
        else:
            print()
    
    print(f"\n{'='*60}")
    print("✅ Clustering completed successfully!")
    print(f"{'='*60}\n")


def run(db_path, building_id, n_clusters=None):
    """Entry point"""
    conn = sqlite3.connect(db_path)
    try:
        perform_clustering(conn, building_id, n_clusters)
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Cluster units based on behavior patterns")
    parser.add_argument("--building", required=True, help="Building ID (e.g., B001)")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to database")
    parser.add_argument("--clusters", type=int, default=None, help="Number of clusters (auto if not specified)")
    
    args = parser.parse_args()
    run(args.db, args.building, args.clusters)