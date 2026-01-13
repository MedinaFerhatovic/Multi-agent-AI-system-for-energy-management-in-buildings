"""
Multi-agentski AI sistem za pametno upravljanje energijom u zgradama
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from db_utils import get_db_connection

st.set_page_config(
    page_title="Multi-agentski AI sistem",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
.block-container { padding-top: 1.5rem; max-width: 1600px; }
h1, h2, h3 { color: #1f2937; letter-spacing: -0.02em; }
h1 { font-size: 1.8rem; font-weight: 800; margin-bottom: 0.5rem; }
.subtitle { font-size: 0.95rem; color: #6b7280; margin-bottom: 1.5rem; }

.panel {
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 16px;
  background: white;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  margin-bottom: 1rem;
}

.alert-card {
    border-radius: 8px;
    padding: 12px;
    margin-bottom: 8px;
    border-left: 4px solid;
    background: white;
}
.alert-critical { background: #fef2f2; border-color: #dc2626; }
.alert-high { background: #fffbeb; border-color: #f59e0b; }
.alert-medium { background: #f0fdf4; border-color: #10b981; }
.alert-low { background: #f0f9ff; border-color: #0ea5e9; }

.decision-card {
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 12px;
    margin-bottom: 8px;
    background: white;
}

.unit-card {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 12px;
  background: white;
  min-height: 90px;
}
.status-dot {
  width: 10px; 
  height: 10px; 
  border-radius: 50%; 
  display: inline-block; 
  margin-right: 8px;
}
.dot-ok { background: #10b981; }
.dot-warn { background: #f59e0b; }
.dot-bad { background: #ef4444; }
.dot-na { background: #9ca3af; }

#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

conn = get_db_connection()

@st.cache_data(ttl=120)
def get_buildings():
    return pd.read_sql_query(
        "SELECT building_id, name, units_total, location_text, building_type, insulation_level FROM buildings",
        conn
    )

@st.cache_data(ttl=120)
def get_units(building_id):
    return pd.read_sql_query(
        "SELECT unit_id, unit_number, floor, area_m2_final FROM units WHERE building_id = ? ORDER BY floor DESC, unit_number",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_latest_readings(building_id):
    query = """
    WITH ranked AS (
        SELECT unit_id, sensor_type, value, timestamp,
               ROW_NUMBER() OVER (PARTITION BY unit_id, sensor_type ORDER BY timestamp DESC) as rn
        FROM sensor_readings
        WHERE building_id = ? AND quality_flag = 'ok'
    )
    SELECT unit_id, sensor_type, value, timestamp FROM ranked WHERE rn = 1
    """
    df = pd.read_sql_query(query, conn, params=(building_id,))
    if df.empty:
        return pd.DataFrame()
    pivot = df.pivot(index="unit_id", columns="sensor_type", values="value")
    pivot["timestamp"] = df.groupby("unit_id")["timestamp"].first()
    return pivot.reset_index()

@st.cache_data(ttl=120)
def get_all_anomalies(building_id):
    """ALL anomalies, no time limit"""
    return pd.read_sql_query(
        """SELECT timestamp, unit_id, anomaly_type, severity, value, action_taken 
           FROM anomalies_log WHERE building_id = ? ORDER BY timestamp DESC""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_all_decisions(building_id):
    """ALL decisions, no time limit"""
    return pd.read_sql_query(
        """SELECT timestamp, unit_id, action, approved, confidence, reasoning_text, mode
           FROM decisions_log WHERE building_id = ? ORDER BY timestamp DESC""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_all_consumption(building_id):
    """ALL consumption data"""
    df = pd.read_sql_query(
        """SELECT timestamp, unit_id, value as kwh
           FROM sensor_readings 
           WHERE building_id = ? AND sensor_type = 'energy' AND quality_flag = 'ok'
           ORDER BY timestamp""",
        conn, params=(building_id,)
    )
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

@st.cache_data(ttl=120)
def get_tariff(building_id):
    return pd.read_sql_query(
        "SELECT * FROM tariff_model WHERE building_id = ?",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_predictions(building_id):
    """ALL predictions"""
    return pd.read_sql_query(
        """SELECT timestamp_created, timestamp_target, unit_id, 
                  predicted_consumption, predicted_occupancy_prob, confidence
           FROM predictions WHERE building_id = ? ORDER BY timestamp_created DESC""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_optimization_plans(building_id):
    """ALL optimization plans"""
    return pd.read_sql_query(
        """SELECT timestamp, unit_id, action_type, target_temp, 
                  estimated_cost, estimated_savings, method
           FROM optimization_plans WHERE building_id = ? ORDER BY timestamp DESC""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_clusters(building_id):
    return pd.read_sql_query(
        """SELECT c.cluster_name, COUNT(uca.unit_id) as unit_count
           FROM unit_cluster_assignment uca
           JOIN clusters c ON uca.cluster_id = c.cluster_id
           WHERE uca.building_id = ? AND (uca.end_date IS NULL OR uca.end_date > datetime('now'))
           GROUP BY c.cluster_name""",
        conn, params=(building_id,)
    )

@st.cache_data(ttl=120)
def get_validation_status(building_id):
    result = pd.read_sql_query(
        "SELECT status, model_confidence_avg, coverage FROM system_validation_log WHERE building_id = ? ORDER BY timestamp DESC LIMIT 1",
        conn, params=(building_id,)
    )
    return result.iloc[0] if not result.empty else None

@st.cache_data(ttl=120)
def get_daily_features(building_id):
    """ALL daily features"""
    return pd.read_sql_query(
        """SELECT date, unit_id, 
                  avg_occupancy_morning, avg_occupancy_daytime, avg_occupancy_evening,
                  weekday_consumption_avg, weekend_consumption_avg, 
                  peak_hour_morning, peak_hour_evening, temp_sensitivity
           FROM unit_features_daily WHERE building_id = ? ORDER BY date DESC""",
        conn, params=(building_id,)
    )

def calculate_costs(consumption_df, tariff_df):
    """Calculate costs based on tariff"""
    if consumption_df.empty or tariff_df.empty:
        return 0, 0, 0
    
    tariff = tariff_df.iloc[0]
    low_price = tariff['low_price_per_kwh']
    high_price = tariff['high_price_per_kwh']
    
    total_kwh = consumption_df['kwh'].sum()
    low_kwh = total_kwh * 0.4
    high_kwh = total_kwh * 0.6
    
    low_cost = low_kwh * low_price
    high_cost = high_kwh * high_price
    total_cost = low_cost + high_cost
    
    return total_cost, low_cost, high_cost

def render_header():
    st.markdown("# Multi-agentski AI sistem za pametno upravljanje energijom u zgradama")
    st.markdown('<div class="subtitle">Napomena: Podaci nisu real-time.</div>', unsafe_allow_html=True)
    st.markdown("---")

    col1, col2, col3 = st.columns([5, 2, 2])
    
    with col1:
        buildings = get_buildings()
        options = [f"{row['building_id']} - {row['name']} ({row['units_total']} stanova, {row['building_type']}, {row['insulation_level']} izolacija)" 
                   for _, row in buildings.iterrows()]
        selected_idx = st.selectbox("Izaberite zgradu", range(len(options)), format_func=lambda i: options[i])
        st.session_state["selected_building"] = buildings.iloc[selected_idx]["building_id"]
    
    building_id = st.session_state["selected_building"]
    validation = get_validation_status(building_id)
    
    with col2:
        if validation is not None:
            st.metric("Status Sistema", validation["status"].upper())
            st.caption(f"Confidence: {validation['model_confidence_avg']:.2f}")
        else:
            st.metric("Status Sistema", "N/A")
    
    with col3:
        if validation is not None and validation['coverage'] is not None:
            st.metric("Pokrivenost", f"{validation['coverage']:.1%}")
        else:
            st.metric("Pokrivenost", "N/A")

def render_overview():
    building_id = st.session_state["selected_building"]
    
    st.markdown("### Pregled zgradnih jedinica")
    units = get_units(building_id)
    latest = get_latest_readings(building_id)
    
    for floor in sorted(units["floor"].unique(), reverse=True):
        floor_units = units[units["floor"] == floor]
        st.markdown(f"**Sprat {floor}**")
        
        cols = st.columns(min(len(floor_units), 6))
        for idx, (_, unit) in enumerate(floor_units.iterrows()):
            unit_data = latest[latest["unit_id"] == unit["unit_id"]] if not latest.empty else pd.DataFrame()
            
            if unit_data.empty:
                status, temp, energy, dot = "N/A", "N/A", "N/A", "dot-na"
            else:
                row = unit_data.iloc[0]
                e = float(row.get("energy", 0) or 0)
                temp = f"{float(row.get('temp_internal')):.1f} C" if pd.notna(row.get("temp_internal")) else "N/A"
                energy = f"{e:.2f} kWh" if pd.notna(row.get("energy")) else "N/A"
                
                if e > 1.5:
                    status, dot = "Visoka", "dot-bad"
                elif e > 0.8:
                    status, dot = "Srednja", "dot-warn"
                else:
                    status, dot = "Normalna", "dot-ok"
            
            with cols[idx % len(cols)]:
                st.markdown(f"""
                <div class="unit-card">
                  <div><span class="status-dot {dot}"></span><b>Stan {unit['unit_number']}</b></div>
                  <div style="font-size:0.9rem; color:#6b7280; margin-top:4px;">
                    Status: {status}<br>
                    Temp: {temp}<br>
                    Energija: {energy}
                  </div>
                </div>
                """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("### Aktivni Alerti")
    anomalies = get_all_anomalies(building_id)
    
    if anomalies.empty:
        st.success("Nema zabilježenih alertova")
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupno Alertova", len(anomalies))
        with col2:
            st.metric("Kritičnih", len(anomalies[anomalies["severity"] == "critical"]))
        with col3:
            st.metric("Visokih", len(anomalies[anomalies["severity"] == "high"]))
        with col4:
            st.metric("Srednjih", len(anomalies[anomalies["severity"] == "medium"]))
        
        st.markdown("**Najnovijih 20 alertova:**")
        for _, alert in anomalies.head(20).iterrows():
            severity = alert['severity']
            st.markdown(f"""
            <div class="alert-card alert-{severity}">
                <b>{alert['anomaly_type'].replace('_', ' ').title()}</b> 
                (Stan {alert['unit_id'].split('_')[-1]})
                <br><small>{alert['timestamp']} | Vrijednost: {alert['value']:.2f} | Akcija: {alert['action_taken'] or 'N/A'}</small>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("### AI Odluke")
    decisions = get_all_decisions(building_id)
    
    if not decisions.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupno Odluka", len(decisions))
        with col2:
            st.metric("Odobreno", decisions["approved"].sum())
        with col3:
            st.metric("Blokirano", len(decisions) - decisions["approved"].sum())
        with col4:
            st.metric("Avg Confidence", f"{decisions['confidence'].mean():.2f}")
        
        st.markdown("**Najnovijih 15 odluka:**")
        for _, dec in decisions.head(15).iterrows():
            approved = "Odobreno" if dec['approved'] else "Blokirano"
            color = "#d1fae5" if dec['approved'] else "#fee2e2"
            st.markdown(f"""
            <div class="decision-card" style="background:{color};">
                <b>Stan {dec['unit_id'].split('_')[-1]}</b> - {dec['action']} 
                ({approved}, Conf: {dec['confidence']:.2f})
                <br><small>{dec['timestamp']} | Razlog: {dec['reasoning_text'] or 'N/A'}</small>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Nema zabilježenih odluka")

def render_analytics():
    building_id = st.session_state["selected_building"]

    st.markdown("### Analiza Potrošnje")
    consumption = get_all_consumption(building_id)
    
    if not consumption.empty:
        total_df = consumption.groupby("timestamp")["kwh"].sum().reset_index()
        total_df["timestamp"] = pd.to_datetime(total_df["timestamp"])
        
        daily = total_df.set_index("timestamp").resample("D").sum().reset_index()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupna Potrošnja", f"{daily['kwh'].sum():.1f} kWh")
        with col2:
            st.metric("Prosjek Dnevni", f"{daily['kwh'].mean():.1f} kWh")
        with col3:
            st.metric("Peak Dan", f"{daily['kwh'].max():.1f} kWh")
        with col4:
            st.metric("Min Dan", f"{daily['kwh'].min():.1f} kWh")
        
        fig = px.bar(daily, x="timestamp", y="kwh", title="Dnevna Potrošnja")
        avg = daily['kwh'].mean()
        fig.add_hline(y=avg, line_dash="dash", annotation_text=f"Prosjek: {avg:.1f} kWh")
        fig.update_layout(showlegend=False, xaxis_title="Datum", yaxis_title="kWh")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        tariff = get_tariff(building_id)
        if not tariff.empty:
            total_cost, low_cost, high_cost = calculate_costs(consumption, tariff)
            
            st.markdown("#### Procjena Troškova")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Ukupni Trošak", f"{total_cost:.2f} BAM")
            with col2:
                st.metric("Niska Tarifa", f"{low_cost:.2f} BAM")
            with col3:
                st.metric("Visoka Tarifa", f"{high_cost:.2f} BAM")
    else:
        st.warning("Nema podataka o potrošnji")
    
    st.markdown("---")
    
    st.markdown("### Dnevne Karakteristike")
    features = get_daily_features(building_id)
    
    if not features.empty:
        st.dataframe(features.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Nema feature podataka")

def render_predictions():
    building_id = st.session_state["selected_building"]
    
    st.markdown("### Predviđanja Modela")
    predictions = get_predictions(building_id)
    
    if not predictions.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Ukupno Predviđanja", len(predictions))
        with col2:
            st.metric("Avg Predicted kWh", f"{predictions['predicted_consumption'].mean():.2f}")
        with col3:
            st.metric("Avg Confidence", f"{predictions['confidence'].mean():.2f}")
        
        st.dataframe(predictions.head(100), use_container_width=True, hide_index=True)
    else:
        st.info("Nema predviđanja")
    
    st.markdown("---")
    
    st.markdown("### Optimizacioni Planovi")
    plans = get_optimization_plans(building_id)
    
    if not plans.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupno Planova", len(plans))
        with col2:
            st.metric("Ukupni Procjenjeni Trošak", f"{plans['estimated_cost'].sum():.2f} BAM")
        with col3:
            st.metric("Ukupne Procjenjene Uštede", f"{plans['estimated_savings'].sum():.2f} BAM")
        with col4:
            net_savings = plans['estimated_savings'].sum() - plans['estimated_cost'].sum()
            st.metric("Neto Uštede", f"{net_savings:.2f} BAM")
        
        st.dataframe(plans.head(100), use_container_width=True, hide_index=True)
    else:
        st.info("Nema optimizacionih planova")

def main():
    if "selected_building" not in st.session_state:
        buildings = get_buildings()
        if not buildings.empty:
            st.session_state["selected_building"] = buildings.iloc[0]["building_id"]
    
    render_header()
    
    tab1, tab2, tab3 = st.tabs(["Pregled", "Analitika", "Predviđanja i Optimizacija"])
    
    with tab1:
        render_overview()
    with tab2:
        render_analytics()
    with tab3:
        render_predictions()

if __name__ == "__main__":
    main()