"""
Detalji Stanova
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db_utils import get_db_connection

st.set_page_config(page_title="Detalji Stana", layout="wide")

st.markdown("""
<style>
.block-container { padding-top: 1.5rem; max-width: 1600px; }
h1, h2, h3 { color: #1f2937; }
.alert-card {
    border-radius: 8px;
    padding: 12px;
    margin-bottom: 8px;
    border-left: 4px solid;
}
.alert-critical { background: #fef2f2; border-color: #dc2626; }
.alert-high { background: #fffbeb; border-color: #f59e0b; }
.alert-medium { background: #f0fdf4; border-color: #10b981; }
</style>
""", unsafe_allow_html=True)

conn = get_db_connection()

@st.cache_data(ttl=120)
def get_units_list(building_id):
    return pd.read_sql_query(
        "SELECT unit_id, unit_number, floor FROM units WHERE building_id = ? ORDER BY floor DESC",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_unit_info(unit_id):
    result = pd.read_sql_query(
        "SELECT u.*, b.name as building_name FROM units u JOIN buildings b ON u.building_id = b.building_id WHERE u.unit_id = ?",
        conn, params=(unit_id,)
    )
    return result.iloc[0] if not result.empty else None

@st.cache_data(ttl=120)
def get_unit_timeseries(unit_id, sensor_type):
    """ALL data for sensor"""
    df = pd.read_sql_query(
        "SELECT timestamp, value FROM sensor_readings WHERE unit_id = ? AND sensor_type = ? AND quality_flag = 'ok' ORDER BY timestamp",
        conn, params=(unit_id, sensor_type)
    )
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

@st.cache_data(ttl=120)
def get_unit_alerts(unit_id):
    """ALL alerts"""
    return pd.read_sql_query(
        "SELECT timestamp, anomaly_type, severity, value FROM anomalies_log WHERE unit_id = ? ORDER BY timestamp DESC",
        conn, params=(unit_id,)
    )

@st.cache_data(ttl=120)
def get_unit_decisions(unit_id):
    """ALL decisions"""
    return pd.read_sql_query(
        "SELECT timestamp, action, approved, confidence, reasoning_text FROM decisions_log WHERE unit_id = ? ORDER BY timestamp DESC",
        conn, params=(unit_id,)
    )

@st.cache_data(ttl=120)
def get_unit_predictions(unit_id):
    """ALL predictions"""
    return pd.read_sql_query(
        "SELECT timestamp_created, timestamp_target, predicted_consumption, predicted_occupancy_prob, confidence FROM predictions WHERE unit_id = ? ORDER BY timestamp_created DESC",
        conn, params=(unit_id,)
    )

@st.cache_data(ttl=120)
def get_unit_optimization(unit_id):
    """ALL optimization plans"""
    return pd.read_sql_query(
        "SELECT timestamp, action_type, target_temp, estimated_cost, estimated_savings FROM optimization_plans WHERE unit_id = ? ORDER BY timestamp DESC",
        conn, params=(unit_id,)
    )

@st.cache_data(ttl=120)
def get_unit_cluster(unit_id):
    result = pd.read_sql_query(
        """SELECT c.cluster_name, uca.confidence FROM unit_cluster_assignment uca
           JOIN clusters c ON uca.cluster_id = c.cluster_id
           WHERE uca.unit_id = ? AND (uca.end_date IS NULL OR uca.end_date > datetime('now'))
           ORDER BY uca.start_date DESC LIMIT 1""",
        conn, params=(unit_id,)
    )
    return result.iloc[0] if not result.empty else None

@st.cache_data(ttl=120)
def get_unit_daily_features(unit_id):
    """ALL daily features"""
    return pd.read_sql_query(
        """SELECT date, avg_occupancy_morning, avg_occupancy_daytime, avg_occupancy_evening,
                  weekday_consumption_avg, weekend_consumption_avg, peak_hour_morning, peak_hour_evening
           FROM unit_features_daily WHERE unit_id = ? ORDER BY date DESC""",
        conn, params=(unit_id,)
    )

def main():
    st.markdown("# Detalji Stana")
    st.markdown("---")
    
    if "selected_building" not in st.session_state:
        st.error("Molimo prvo izaberite zgradu na glavnoj stranici")
        st.stop()
    
    building_id = st.session_state["selected_building"]
    units = get_units_list(building_id)
    
    if units.empty:
        st.warning("Nema stanova")
        st.stop()
    
    options = [f"Stan {row['unit_number']} (Sprat {row['floor']})" for _, row in units.iterrows()]
    selected_idx = st.selectbox("Izaberite stan", range(len(options)), format_func=lambda i: options[i])
    unit_id = units.iloc[selected_idx]["unit_id"]
    
    st.markdown("---")
    
    unit_info = get_unit_info(unit_id)
    cluster = get_unit_cluster(unit_id)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Stan", unit_info["unit_number"])
    with col2:
        st.metric("Sprat", unit_info["floor"])
    with col3:
        area = unit_info["area_m2_final"] or unit_info["area_m2_estimated"] or "N/A"
        st.metric("Površina", f"{area} m²" if area != "N/A" else "N/A")
    with col4:
        st.metric("Klaster", cluster["cluster_name"] if cluster is not None else "N/A")
    
    st.markdown("---")

    st.markdown("### Potrošnja Energije")
    energy = get_unit_timeseries(unit_id, "energy")
    
    if not energy.empty:
        fig = px.line(energy, x="timestamp", y="value", labels={"value": "kWh"})
        avg = energy["value"].mean()
        fig.add_hline(y=avg, line_dash="dash", annotation_text=f"Prosjek: {avg:.2f}")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupno", f"{energy['value'].sum():.2f} kWh")
        with col2:
            st.metric("Prosjek", f"{avg:.2f} kWh")
        with col3:
            st.metric("Max", f"{energy['value'].max():.2f} kWh")
        with col4:
            st.metric("Min", f"{energy['value'].min():.2f} kWh")
    else:
        st.warning("Nema podataka")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Temperatura")
        temp = get_unit_timeseries(unit_id, "temp_internal")
        if not temp.empty:
            fig = px.line(temp, x="timestamp", y="value", labels={"value": "C"})
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            st.info(f"Prosjek: {temp['value'].mean():.1f} C")
        else:
            st.warning("Nema podataka")
    
    with col2:
        st.markdown("#### Popunjenost")
        occ = get_unit_timeseries(unit_id, "occupancy")
        if not occ.empty:
            occ["percentage"] = occ["value"] * 100
            fig = px.area(occ, x="timestamp", y="percentage")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            st.info(f"Prosjek: {occ['value'].mean():.0%}")
        else:
            st.warning("Nema podataka")
    
    st.markdown("---")

    st.markdown("### Alerti")
    alerts = get_unit_alerts(unit_id)
    
    if not alerts.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Kritičnih", len(alerts[alerts["severity"] == "critical"]))
        with col2:
            st.metric("Visokih", len(alerts[alerts["severity"] == "high"]))
        with col3:
            st.metric("Srednjih", len(alerts[alerts["severity"] == "medium"]))
        
        for _, alert in alerts.head(30).iterrows():
            st.markdown(f"""
            <div class="alert-card alert-{alert['severity']}">
                <b>{alert['anomaly_type'].replace('_', ' ').title()}</b><br>
                <small>{alert['timestamp']} | Vrijednost: {alert['value']:.2f}</small>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("Nema alertova")
    
    st.markdown("---")
    
    st.markdown("### AI Odluke")
    decisions = get_unit_decisions(unit_id)
    
    if not decisions.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Ukupno", len(decisions))
        with col2:
            st.metric("Odobreno", decisions["approved"].sum())
        with col3:
            st.metric("Avg Confidence", f"{decisions['confidence'].mean():.2f}")
        
        st.dataframe(decisions.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Nema odluka")
    
    st.markdown("---")
    
    st.markdown("### Predviđanja")
    predictions = get_unit_predictions(unit_id)
    
    if not predictions.empty:
        st.dataframe(predictions.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Nema predviđanja")
    
    st.markdown("---")
    
    st.markdown("### Optimizacioni Planovi")
    optimization = get_unit_optimization(unit_id)
    
    if not optimization.empty:
        st.dataframe(optimization.head(50), use_container_width=True, hide_index=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Ukupni Procjenjeni Trošak", f"{optimization['estimated_cost'].sum():.2f} BAM")
        with col2:
            st.metric("Ukupne Procjenjene Uštede", f"{optimization['estimated_savings'].sum():.2f} BAM")
    else:
        st.info("Nema planova")
    
    st.markdown("---")

    st.markdown("### Dnevne Karakteristike")
    features = get_unit_daily_features(unit_id)
    
    if not features.empty:
        st.dataframe(features.head(100), use_container_width=True, hide_index=True)
    else:
        st.info("Nema feature podataka")

if __name__ == "__main__":
    main()