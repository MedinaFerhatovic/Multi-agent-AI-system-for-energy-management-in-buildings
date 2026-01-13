"""
Detalji stana - detaljni prikaz za pojedinačni stan
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from db_utils import get_db_connection

st.set_page_config(page_title="Detalji stana", page_icon="", layout="wide")

# ============================================
# CLEAN DESIGN (CSS only)
# ============================================
st.markdown(
    """
<style>
/* Layout */
.block-container { padding-top: 1.6rem; padding-bottom: 2.2rem; max-width: 1320px; }
hr { margin: 0.85rem 0 1.1rem 0; }

/* Typography */
h1, h2, h3 { letter-spacing: -0.02em; }
.muted { color: rgba(49, 51, 63, 0.65); font-size: 0.95rem; line-height: 1.35; }
.kicker { color: rgba(49, 51, 63, 0.70); font-size: 0.92rem; margin-top: -0.25rem; }

/* Panels */
.panel {
  border: 1px solid rgba(49, 51, 63, 0.12);
  border-radius: 16px;
  padding: 14px 14px 12px 14px;
  background: rgba(255, 255, 255, 0.72);
  box-shadow: 0 1px 10px rgba(0,0,0,0.04);
}

/* Section headings */
.section-title { font-size: 1.15rem; font-weight: 760; margin-bottom: 0.35rem; }
.section-sub { color: rgba(49, 51, 63, 0.65); font-size: 0.92rem; margin-bottom: 0.6rem; }

/* Badges */
.badge {
  display: inline-block;
  padding: 0.22rem 0.55rem;
  border-radius: 999px;
  border: 1px solid rgba(49, 51, 63, 0.15);
  background: rgba(49, 51, 63, 0.03);
  font-size: 0.82rem;
  color: rgba(49, 51, 63, 0.78);
}

/* Metric cards wrapper */
.metric-card {
  border: 1px solid rgba(49, 51, 63, 0.12);
  border-radius: 16px;
  padding: 12px 12px 10px 12px;
  background: #ffffff;
  box-shadow: 0 1px 10px rgba(0,0,0,0.05);
}

/* Reduce Streamlit element spacing inside expanders */
div[data-testid="stExpander"] > div { border-radius: 16px; }
</style>
""",
    unsafe_allow_html=True,
)

# ============================================
# DATABASE CONNECTION
# ============================================
conn = get_db_connection()

# ============================================
# QUERIES (unchanged)
# ============================================
@st.cache_data(ttl=60)
def get_units_list(building_id):
    query = """
    SELECT unit_id, unit_number, floor, area_m2_final
    FROM units
    WHERE building_id = ?
    ORDER BY floor DESC, unit_number
    """
    return pd.read_sql_query(query, conn, params=(building_id,))


@st.cache_data(ttl=60)
def get_unit_info(unit_id):
    query = """
    SELECT 
        u.*,
        b.name as building_name,
        b.location_text
    FROM units u
    JOIN buildings b ON u.building_id = b.building_id
    WHERE u.unit_id = ?
    """
    result = pd.read_sql_query(query, conn, params=(unit_id,))
    return result.iloc[0] if not result.empty else None


@st.cache_data(ttl=60)
def get_unit_timeseries(unit_id, sensor_type, hours=168):
    """Vremenska serija za određeni senzor (7 dana je podrazumijevano)"""
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()

    query = """
    SELECT timestamp, value
    FROM sensor_readings
    WHERE unit_id = ?
    AND sensor_type = ?
    AND quality_flag = 'ok'
    AND timestamp >= ?
    ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(unit_id, sensor_type, cutoff))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@st.cache_data(ttl=60)
def get_unit_alerts(unit_id, days=7):
    """Alerti za stan"""
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    query = """
    SELECT 
        timestamp,
        anomaly_type,
        severity,
        value,
        action_taken
    FROM anomalies_log
    WHERE unit_id = ?
    AND timestamp >= ?
    ORDER BY timestamp DESC
    """
    return pd.read_sql_query(query, conn, params=(unit_id, cutoff))


@st.cache_data(ttl=60)
def get_unit_decisions(unit_id, days=7):
    """AI odluke za stan"""
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    query = """
    SELECT 
        timestamp,
        action,
        approved,
        confidence,
        reasoning_text
    FROM decisions_log
    WHERE unit_id = ?
    AND timestamp >= ?
    ORDER BY timestamp DESC
    """
    return pd.read_sql_query(query, conn, params=(unit_id, cutoff))


@st.cache_data(ttl=60)
def get_unit_cluster_info(unit_id):
    """Dodjela klastera"""
    query = """
    SELECT 
        c.cluster_name,
        uca.confidence,
        uca.start_date,
        uca.reason
    FROM unit_cluster_assignment uca
    JOIN clusters c ON uca.cluster_id = c.cluster_id
    WHERE uca.unit_id = ?
    AND (uca.end_date IS NULL OR uca.end_date > datetime('now'))
    ORDER BY uca.start_date DESC
    LIMIT 1
    """
    result = pd.read_sql_query(query, conn, params=(unit_id,))
    return result.iloc[0] if not result.empty else None


@st.cache_data(ttl=60)
def get_unit_daily_stats(unit_id, days=30):
    """Dnevna statistika"""
    cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()

    query = """
    SELECT 
        date,
        avg_occupancy_morning,
        avg_occupancy_daytime,
        avg_occupancy_evening,
        weekday_consumption_avg,
        weekend_consumption_avg
    FROM unit_features_daily
    WHERE unit_id = ?
    AND date >= ?
    ORDER BY date DESC
    """
    return pd.read_sql_query(query, conn, params=(unit_id, cutoff))


# ============================================
# MAIN PAGE
# ============================================
def main():
    st.markdown("## Detalji stana")
    st.markdown(
        '<div class="kicker">Detaljni prikaz historijskih podataka iz baze za pojedinačni stan.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # Check if building is selected (unchanged logic)
    if "selected_building" not in st.session_state:
        st.error("Molimo prvo izaberite zgradu na stranici Pregled.")
        st.stop()

    building_id = st.session_state["selected_building"]

    # Unit selector (unchanged logic)
    units = get_units_list(building_id)

    if units.empty:
        st.warning("Nema stanova za odabranu zgradu.")
        st.stop()

    unit_options = {
        f"Stan {row['unit_number']} (Sprat {row['floor']})": row["unit_id"]
        for _, row in units.iterrows()
    }

    selected = st.selectbox("Izaberite stan", list(unit_options.keys()))
    unit_id = unit_options[selected]

    st.markdown("---")

    # Load unit info (unchanged logic)
    unit_info = get_unit_info(unit_id)
    cluster_info = get_unit_cluster_info(unit_id)

    # ============================================
    # HEADER SECTION (same metrics, cleaner layout)
    # ============================================
    st.markdown('<div class="section-title">Pregled</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Osnovne informacije o stanu i pripadnosti klasteru.</div>',
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Broj stana", unit_info["unit_number"])
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.metric("Sprat", unit_info["floor"])
        st.markdown("</div>", unsafe_allow_html=True)

    with col3:
        area = unit_info["area_m2_final"] or unit_info["area_m2_estimated"] or "N/A"
        st.metric("Površina", f"{area} m²" if area != "N/A" else "N/A")
        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        if cluster_info is not None:
            st.metric("Klaster", cluster_info["cluster_name"])
        else:
            st.metric("Klaster", "N/A")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ============================================
    # TIME RANGE SELECTOR (unchanged logic)
    # ============================================
    st.markdown('<div class="section-title">Vremenski period</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Odaberite vremenski period za grafove i tabele.</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        time_range = st.selectbox(
            "Period",
            options=[("24 sata", 24), ("3 dana", 72), ("7 dana", 168), ("30 dana", 720)],
            format_func=lambda x: x[0],
            index=2,
        )
        hours = time_range[1]

    st.markdown("---")

    # ============================================
    # ENERGY CONSUMPTION (same logic; no emoji)
    # ============================================
    st.markdown('<div class="section-title">Potrošnja energije</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Historijska potrošnja energije za izabrani period.</div>',
        unsafe_allow_html=True,
    )

    energy_data = get_unit_timeseries(unit_id, "energy", hours=hours)

    if not energy_data.empty:
        fig = px.line(
            energy_data,
            x="timestamp",
            y="value",
            title=f"Potrošnja energije ({time_range[0]})",
            labels={"value": "kWh", "timestamp": "Vrijeme"},
        )

        avg = energy_data["value"].mean()
        fig.add_hline(
            y=avg,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Prosjek: {avg:.2f} kWh",
        )

        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ukupno", f"{energy_data['value'].sum():.2f} kWh")
        with col2:
            st.metric("Prosjek", f"{avg:.2f} kWh")
        with col3:
            st.metric("Maksimum", f"{energy_data['value'].max():.2f} kWh")
        with col4:
            st.metric("Minimum", f"{energy_data['value'].min():.2f} kWh")
    else:
        st.warning("Nema podataka o potrošnji.")

    st.markdown("---")

    # ============================================
    # TEMPERATURE & OCCUPANCY (same logic; no emoji)
    # ============================================
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-title">Temperatura</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Interna temperatura (temp_internal).</div>', unsafe_allow_html=True)

        temp_data = get_unit_timeseries(unit_id, "temp_internal", hours=hours)

        if not temp_data.empty:
            fig = px.line(
                temp_data,
                x="timestamp",
                y="value",
                title="Interna temperatura",
                labels={"value": "°C", "timestamp": "Vrijeme"},
            )
            fig.update_layout(hovermode="x unified", height=300)
            st.plotly_chart(fig, use_container_width=True)

            avg_temp = temp_data["value"].mean()
            st.markdown(
                f'<div class="panel"><span class="badge">Sažetak</span><div class="muted" style="margin-top:8px;">Prosječna temperatura: <b>{avg_temp:.1f}°C</b></div></div>',
                unsafe_allow_html=True,
            )
        else:
            st.warning("Nema podataka o temperaturi.")

    with col2:
        st.markdown('<div class="section-title">Popunjenost</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Vjerovatnoća prisutnosti (occupancy).</div>', unsafe_allow_html=True)

        occ_data = get_unit_timeseries(unit_id, "occupancy", hours=hours)

        if not occ_data.empty:
            occ_data = occ_data.copy()
            occ_data["percentage"] = occ_data["value"] * 100

            fig = px.area(
                occ_data,
                x="timestamp",
                y="percentage",
                title="Vjerovatnoća prisutnosti",
                labels={"percentage": "%", "timestamp": "Vrijeme"},
            )
            fig.update_layout(hovermode="x unified", height=300)
            st.plotly_chart(fig, use_container_width=True)

            avg_occ = occ_data["value"].mean()
            st.markdown(
                f'<div class="panel"><span class="badge">Sažetak</span><div class="muted" style="margin-top:8px;">Prosječna popunjenost: <b>{avg_occ:.0%}</b></div></div>',
                unsafe_allow_html=True,
            )
        else:
            st.warning("Nema podataka o popunjenosti.")

    st.markdown("---")

    # ============================================
    # ALERTS (same logic; no emoji)
    # ============================================
    st.markdown('<div class="section-title">Alerti (zadnjih 7 dana)</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Anomalije zabilježene u anomalies_log.</div>', unsafe_allow_html=True)

    alerts = get_unit_alerts(unit_id, days=7)

    if alerts.empty:
        st.success("Nema alertova za ovaj period.")
    else:
        col1, col2, col3 = st.columns(3)
        severity_counts = alerts["severity"].value_counts()

        with col1:
            st.metric("Kritično", severity_counts.get("critical", 0))
        with col2:
            st.metric("Visoko", severity_counts.get("high", 0))
        with col3:
            st.metric(
                "Srednje/Nisko",
                severity_counts.get("medium", 0) + severity_counts.get("low", 0),
            )

        st.dataframe(alerts, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ============================================
    # AI DECISIONS (same logic; no emoji)
    # ============================================
    st.markdown('<div class="section-title">AI odluke (zadnjih 7 dana)</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Odluke iz decisions_log za izabrani stan.</div>', unsafe_allow_html=True)

    decisions = get_unit_decisions(unit_id, days=7)

    if decisions.empty:
        st.info("Nema AI odluka za ovaj period.")
    else:
        col1, col2, col3 = st.columns(3)

        with col1:
            approved_count = decisions["approved"].sum()
            st.metric("Odobreno", approved_count)

        with col2:
            blocked_count = len(decisions) - approved_count
            st.metric("Blokirano", blocked_count)

        with col3:
            avg_conf = decisions["confidence"].mean()
            st.metric("Prosječna pouzdanost", f"{avg_conf:.2f}")

        st.dataframe(
            decisions[["timestamp", "action", "approved", "confidence"]],
            use_container_width=True,
            hide_index=True,
        )

        with st.expander("Obrazloženje (zadnjih 10)"):
            for _, row in decisions.head(10).iterrows():
                approved_txt = "Da" if row["approved"] else "Ne"
                st.markdown(
                    f"""
**{row['timestamp']}** — `{row['action']}`
- Odobreno: {approved_txt}
- Pouzdanost: {row['confidence']:.2f}
- Obrazloženje: {row['reasoning_text'] or 'N/A'}
---
"""
                )

    st.markdown("---")

    # ============================================
    # DAILY PATTERNS (same logic; no emoji)
    # ============================================
    st.markdown('<div class="section-title">Dnevni obrasci (zadnjih 30 dana)</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Agregirane karakteristike iz unit_features_daily.</div>', unsafe_allow_html=True)

    daily_stats = get_unit_daily_stats(unit_id, days=30)

    if not daily_stats.empty:
        daily_stats = daily_stats.copy()
        daily_stats["date"] = pd.to_datetime(daily_stats["date"])

        fig = go.Figure()

        if "weekday_consumption_avg" in daily_stats.columns:
            fig.add_trace(
                go.Scatter(
                    x=daily_stats["date"],
                    y=daily_stats["weekday_consumption_avg"],
                    mode="lines",
                    name="Radni dan",
                    line=dict(color="blue"),
                )
            )

        if "weekend_consumption_avg" in daily_stats.columns:
            fig.add_trace(
                go.Scatter(
                    x=daily_stats["date"],
                    y=daily_stats["weekend_consumption_avg"],
                    mode="lines",
                    name="Vikend",
                    line=dict(color="orange"),
                )
            )

        fig.update_layout(
            title="Radni dan naspram vikenda (potrošnja)",
            xaxis_title="Datum",
            yaxis_title="kWh",
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        if all(
            col in daily_stats.columns
            for col in [
                "avg_occupancy_morning",
                "avg_occupancy_daytime",
                "avg_occupancy_evening",
            ]
        ):
            occ_avg = daily_stats[
                ["avg_occupancy_morning", "avg_occupancy_daytime", "avg_occupancy_evening"]
            ].mean()

            fig = go.Figure(
                data=[
                    go.Bar(
                        x=["Jutro (06-08)", "Danje (08-17)", "Večer (17-22)"],
                        y=occ_avg * 100,
                    )
                ]
            )
            fig.update_layout(
                title="Prosječna popunjenost po dobu dana",
                yaxis_title="%",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nema dnevnih statistika.")


if __name__ == "__main__":
    main()
