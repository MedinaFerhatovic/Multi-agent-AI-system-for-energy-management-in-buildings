"""
Sistem (Admin) 
"""
import streamlit as st
import pandas as pd
import json
from db_utils import get_db_connection

st.set_page_config(page_title="Sistem", layout="wide")

st.markdown("""
<style>
.block-container { padding-top: 1.5rem; max-width: 1600px; }
h1, h2, h3 { color: #1f2937; }
</style>
""", unsafe_allow_html=True)

conn = get_db_connection()

def get_table_count(table_name):
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except:
        return None

@st.cache_data(ttl=120)
def get_data_quality(building_id):
    return pd.read_sql_query(
        """SELECT sensor_type, quality_flag, COUNT(*) as count
           FROM sensor_readings WHERE building_id = ?
           GROUP BY sensor_type, quality_flag""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_models():
    df = pd.read_sql_query(
        "SELECT model_id, model_type, trained_at, is_active, metrics_json FROM model_registry ORDER BY trained_at DESC",
        conn
    )
    if not df.empty:
        df["metrics"] = df["metrics_json"].apply(lambda x: json.loads(x) if x else {})
    return df

@st.cache_data(ttl=120)
def get_pipeline():
    return pd.read_sql_query(
        "SELECT pipeline_name, building_id, current_anchor_ts, updated_at FROM pipeline_progress ORDER BY updated_at DESC",
        conn
    )

@st.cache_data(ttl=120)
def get_all_problems():
    """ALL problems, no time limit"""
    return pd.read_sql_query(
        """SELECT 'anomaly' as type, timestamp, building_id, unit_id, anomaly_type as detail, severity
           FROM anomalies_log WHERE severity IN ('critical', 'high')
           UNION ALL
           SELECT 'decision_blocked' as type, timestamp, building_id, unit_id, action as detail, 'blocked' as severity
           FROM decisions_log WHERE approved = 0
           ORDER BY timestamp DESC""",
        conn
    )

def main():
    st.markdown("# Sistem (Admin)")
    st.markdown("---")
    
    st.markdown("### Pregled Tabela")
    tables = [
        "buildings", "units", "sensors", "sensor_readings", "external_weather",
        "unit_features_daily", "clusters", "predictions", "optimization_plans",
        "decisions_log", "anomalies_log", "model_registry"
    ]
    
    stats = []
    for table in tables:
        count = get_table_count(table)
        stats.append({"Tabela": table, "Redova": count if count is not None else "Greška"})
    
    df = pd.DataFrame(stats)
    col1, col2 = st.columns(2)
    mid = len(df) // 2
    with col1:
        st.dataframe(df.iloc[:mid], use_container_width=True, hide_index=True)
    with col2:
        st.dataframe(df.iloc[mid:], use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    st.markdown("### Kvalitet Podataka")
    buildings = pd.read_sql_query("SELECT building_id, name FROM buildings", conn)
    
    if not buildings.empty:
        building_id = st.selectbox(
            "Zgrada",
            buildings["building_id"].tolist(),
            format_func=lambda x: f"{x} - {buildings[buildings['building_id'] == x]['name'].iloc[0]}"
        )
        
        quality = get_data_quality(building_id)
        
        if not quality.empty:
            for sensor_type in quality["sensor_type"].unique():
                with st.expander(f"{sensor_type.upper()}"):
                    sensor_df = quality[quality["sensor_type"] == sensor_type]
                    ok = sensor_df[sensor_df["quality_flag"] == "ok"]["count"].sum()
                    total = sensor_df["count"].sum()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("OK", f"{ok:,}")
                    with col2:
                        st.metric("Loših", f"{total - ok:,}")
                    with col3:
                        st.metric("Kvalitet", f"{(ok/total*100):.1f}%")
    
    st.markdown("---")
    
    st.markdown("### ML Model Performanse")
    models = get_models()
    
    if not models.empty:
        active = models[models["is_active"] == 1]
        
        if not active.empty:
            st.success(f"Aktivnih Modela: {len(active)}")
            
            for _, model in active.iterrows():
                with st.expander(f"{model['model_id']}"):
                    metrics = model["metrics"].get("test", {})
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        mae = metrics.get("mae", "N/A")
                        st.metric("Test MAE", f"{mae:.4f}" if mae != "N/A" else "N/A")
                    with col2:
                        rmse = metrics.get("rmse", "N/A")
                        st.metric("Test RMSE", f"{rmse:.4f}" if rmse != "N/A" else "N/A")
                    with col3:
                        r2 = metrics.get("r2", "N/A")
                        st.metric("Test R²", f"{r2:.4f}" if r2 != "N/A" else "N/A")
                    
                    st.caption(f"Treniran: {model['trained_at']}")
    
    st.markdown("---")
    
    st.markdown("### Workflow Pipeline")
    pipeline = get_pipeline()
    
    if not pipeline.empty:
        st.dataframe(pipeline, use_container_width=True, hide_index=True)
    else:
        st.info("Nema podataka")
    
    st.markdown("---")
    
    st.markdown("### Problemi (Svi)")
    problems = get_all_problems()
    
    if not problems.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Kritične Anomalije", len(problems[problems["type"] == "anomaly"]))
        with col2:
            st.metric("Blokirane Odluke", len(problems[problems["type"] == "decision_blocked"]))
        
        st.dataframe(problems.head(200), use_container_width=True, hide_index=True, height=400)
    else:
        st.success("Nema problema")

if __name__ == "__main__":
    main()