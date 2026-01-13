"""
Energy Management Dashboard - Offline Monitoring
Prikazuje historijske podatke iz baze (nema real-time stream)
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from db_utils import get_db_connection, find_db_path

# ============================================
# PAGE CONFIG (no emojis)
# ============================================
st.set_page_config(
    page_title="Multi agent AI sistem za upravljanje energijom u zgradama",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================
# CLEAN DESIGN (CSS only, no logic changes)
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

/* Unit cards */
.unit-card {
  border: 1px solid rgba(49, 51, 63, 0.12);
  border-radius: 16px;
  padding: 12px 12px 10px 12px;
  background: #ffffff;
  box-shadow: 0 1px 10px rgba(0,0,0,0.05);
  min-height: 96px;
}
.unit-title { font-weight: 720; font-size: 1.02rem; margin-bottom: 4px; }
.unit-meta { font-size: 0.88rem; color: rgba(49, 51, 63, 0.70); line-height: 1.35; }
.status-dot {
  width: 10px; height: 10px; border-radius: 999px; display:inline-block; margin-right: 8px; transform: translateY(1px);
}
.dot-ok { background: #2ecc71; }
.dot-warn { background: #f1c40f; }
.dot-bad { background: #e74c3c; }
.dot-na { background: #95a5a6; }

/* Sidebar */
section[data-testid="stSidebar"] .stMarkdown { font-size: 0.92rem; }
</style>
""",
    unsafe_allow_html=True,
)

# ============================================
# OFFLINE / SNAPSHOT BANNER (no emojis)
# ============================================
st.info(
    "OFFLINE / SNAPSHOT MOD — Nema real-time monitoringa. "
    "Sve metrike i grafovi su učitani iz SQLite baze i mogu kasniti."
)

# ============================================
# DATABASE CONNECTION
# ============================================
conn = get_db_connection()


def table_exists(table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


# Debug info (collapsible; no emojis)
with st.sidebar.expander("Debug", expanded=False):
    st.write("Putanja baze:", str(find_db_path()))
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", conn
    )
    st.write("Tabele:", tables["name"].tolist())

# ============================================
# TIME HELPERS (anchored to DB snapshot)
# ============================================
def _parse_iso(ts: str) -> datetime:
    s = str(ts).replace("Z", "").replace("T", " ")
    return datetime.fromisoformat(s)


def _cutoff_from_anchor(anchor_ts: str | None, hours: int) -> str | None:
    if not anchor_ts:
        return None
    cutoff = _parse_iso(anchor_ts) - timedelta(hours=hours)
    return cutoff.isoformat(sep=" ")


# ============================================
# HELPER FUNCTIONS (UNCHANGED LOGIC)
# ============================================
@st.cache_data(ttl=60)
def get_buildings():
    query = """
    SELECT building_id, name, units_total, location_text
    FROM buildings
    ORDER BY building_id
    """
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=60)
def get_latest_data_timestamp(building_id):
    query = """
    SELECT MAX(timestamp) as latest
    FROM sensor_readings
    WHERE building_id = ?
      AND quality_flag = 'ok'
    """
    result = pd.read_sql_query(query, conn, params=(building_id,))
    return result["latest"].iloc[0] if not result.empty else None


@st.cache_data(ttl=60)
def get_units_for_building(building_id):
    query = """
    SELECT unit_id, unit_number, floor, area_m2_final
    FROM units
    WHERE building_id = ?
    ORDER BY floor DESC, unit_number
    """
    return pd.read_sql_query(query, conn, params=(building_id,))


@st.cache_data(ttl=60)
def get_latest_readings(building_id, anchor_ts=None):
    """
    Zadnja očitanja senzora za svaki stan (as-of anchor)
    VAŽNO: snapshot podaci iz baze, ne live stream
    """
    if anchor_ts is None:
        anchor_ts = get_latest_data_timestamp(building_id)

    if anchor_ts is None:
        return pd.DataFrame(columns=["unit_id", "timestamp"])

    query = """
    WITH ranked AS (
        SELECT
            unit_id,
            sensor_type,
            value,
            timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY unit_id, sensor_type
                ORDER BY timestamp DESC
            ) as rn
        FROM sensor_readings
        WHERE building_id = ?
          AND timestamp <= ?
          AND quality_flag = 'ok'
    )
    SELECT unit_id, sensor_type, value, timestamp
    FROM ranked
    WHERE rn = 1
    """
    df = pd.read_sql_query(query, conn, params=(building_id, anchor_ts))

    if df.empty:
        return pd.DataFrame(columns=["unit_id", "timestamp"])

    pivot = df.pivot(index="unit_id", columns="sensor_type", values="value")
    pivot["timestamp"] = df.groupby("unit_id")["timestamp"].first()
    return pivot.reset_index()


@st.cache_data(ttl=60)
def get_active_alerts(building_id, hours=24):
    """
    Aktivni alerti (zadnjih X sati) – ali po DB snapshot anchoru (ne real-time).
    """
    anchor_ts = get_latest_data_timestamp(building_id)
    cutoff = _cutoff_from_anchor(anchor_ts, hours)

    if cutoff is None:
        return pd.DataFrame(
            columns=["timestamp", "unit_id", "anomaly_type", "severity", "value", "action_taken"]
        )

    query = """
    SELECT
        timestamp,
        unit_id,
        anomaly_type,
        severity,
        value,
        action_taken
    FROM anomalies_log
    WHERE building_id = ?
      AND timestamp >= ?
    ORDER BY timestamp DESC
    """
    return pd.read_sql_query(query, conn, params=(building_id, cutoff))


@st.cache_data(ttl=60)
def get_consumption_timeseries(building_id, hours=24):
    """
    Historijska potrošnja (agregirano) – prozor je vezan za DB snapshot anchor.
    """
    anchor_ts = get_latest_data_timestamp(building_id)
    cutoff = _cutoff_from_anchor(anchor_ts, hours)

    if cutoff is None:
        return pd.DataFrame(columns=["timestamp", "total_kwh"])

    query = """
    SELECT
        timestamp,
        SUM(value) as total_kwh
    FROM sensor_readings
    WHERE building_id = ?
      AND sensor_type = 'energy'
      AND quality_flag = 'ok'
      AND timestamp >= ?
    GROUP BY timestamp
    ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(building_id, cutoff))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@st.cache_data(ttl=60)
def get_recent_decisions(building_id, hours=24):
    """
    Nedavne AI odluke – prozor po DB snapshot anchoru.
    """
    anchor_ts = get_latest_data_timestamp(building_id)
    cutoff = _cutoff_from_anchor(anchor_ts, hours)

    if cutoff is None:
        return pd.DataFrame(
            columns=["timestamp", "unit_id", "action", "approved", "confidence", "reasoning_text"]
        )

    query = """
    SELECT
        timestamp,
        unit_id,
        action,
        approved,
        confidence,
        reasoning_text
    FROM decisions_log
    WHERE building_id = ?
      AND timestamp >= ?
    ORDER BY timestamp DESC
    LIMIT 50
    """
    return pd.read_sql_query(query, conn, params=(building_id, cutoff))


@st.cache_data(ttl=60)
def get_validation_status(building_id):
    if not table_exists("system_validation_log"):
        return None

    query = """
    SELECT
        timestamp,
        status,
        model_confidence_avg,
        coverage,
        blocked_units_count,
        invalid_units_count,
        reasons_json
    FROM system_validation_log
    WHERE building_id = ?
    ORDER BY timestamp DESC
    LIMIT 1
    """
    result = pd.read_sql_query(query, conn, params=(building_id,))
    return result.iloc[0] if not result.empty else None


# ============================================
# UI CONSTANTS (UNCHANGED LOGIC)
# ============================================
STATUS_COLORS = {
    "ok": "ok",
    "degraded": "warn",
    "blocked": "bad",
    "offline": "na",
}


def _status_to_dot_class(status_text: str) -> str:
    key = str(status_text or "").strip().lower()
    mapped = STATUS_COLORS.get(key, "na")
    if mapped == "ok":
        return "dot-ok"
    if mapped == "warn":
        return "dot-warn"
    if mapped == "bad":
        return "dot-bad"
    return "dot-na"


# ============================================
# TOP BAR (design-only changes)
# ============================================
def render_top_bar():
    st.markdown("## Dashboard upravljanja energijom")
    st.markdown(
        '<div class="kicker">Prikaz je zasnovan na offline snapshot podacima (nema real-time monitoringa).</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    col1, col2, col3, col4 = st.columns([2, 2, 2, 3])

    with col1:
        buildings = get_buildings()
        building_options = {
            f"{row['building_id']} - {row['name']}": row["building_id"]
            for _, row in buildings.iterrows()
        }
        selected = st.selectbox("Zgrada", options=list(building_options.keys()), key="building_selector")
        st.session_state["selected_building"] = building_options[selected]

    building_id = st.session_state["selected_building"]
    latest_ts = get_latest_data_timestamp(building_id)

    validation = get_validation_status(building_id)

    with col2:
        if validation is not None:
            status = str(validation["status"])
            conf = validation["model_confidence_avg"]
            dot = _status_to_dot_class(status)
            conf_text = f"{float(conf):.2f}" if conf is not None else "N/A"
            st.markdown(
                f"""
                <div class="panel">
                  <div class="badge">Status sistema</div>
                  <div style="margin-top:10px;">
                    <span class="status-dot {dot}"></span>
                    <span style="font-weight:700; font-size:1.05rem;">{status.upper()}</span>
                  </div>
                  <div class="muted" style="margin-top:6px;">
                    Pouzdanost (confidence): {conf_text}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            if table_exists("system_validation_log"):
                st.markdown(
                    """
                    <div class="panel">
                      <div class="badge">Status sistema</div>
                      <div style="margin-top:10px;">
                        <span class="status-dot dot-na"></span>
                        <span style="font-weight:700; font-size:1.05rem;">OFFLINE</span>
                      </div>
                      <div class="muted" style="margin-top:6px;">Nema redova u validation logu</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    """
                    <div class="panel">
                      <div class="badge">Status sistema</div>
                      <div style="margin-top:10px;">
                        <span class="status-dot dot-na"></span>
                        <span style="font-weight:700; font-size:1.05rem;">N/A</span>
                      </div>
                      <div class="muted" style="margin-top:6px;">Tabela system_validation_log ne postoji</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    with col3:
        if latest_ts:
            try:
                dt = _parse_iso(latest_ts)
                age = datetime.now() - dt
                st.markdown(
                    f"""
                    <div class="panel">
                      <div class="badge">Zadnji snapshot baze</div>
                      <div style="margin-top:10px; font-weight:700; font-size:1.05rem;">
                        {dt.strftime("%Y-%m-%d %H:%M:%S")}
                      </div>
                      <div class="muted" style="margin-top:6px;">
                        Starost: {int(age.total_seconds()/60)} minuta
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            except Exception:
                st.markdown(
                    f"""
                    <div class="panel">
                      <div class="badge">Zadnji snapshot baze</div>
                      <div style="margin-top:10px; font-weight:700; font-size:1.05rem;">
                        {str(latest_ts)}
                      </div>
                      <div class="muted" style="margin-top:6px;">Starost: N/A</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                """
                <div class="panel">
                  <div class="badge">Zadnji snapshot baze</div>
                  <div style="margin-top:10px; font-weight:700; font-size:1.05rem;">N/A</div>
                  <div class="muted" style="margin-top:6px;">Nema podataka</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with col4:
        st.markdown(
            """
            <div class="panel">
              <div class="badge">Offline monitoring</div>
              <div class="muted" style="margin-top:8px;">
                Prikaz je snapshot iz SQLite baze (historijski podaci). Ne postoji real-time stream.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================
# TAB 1: OVERVIEW (design-only changes)
# ============================================
def render_overview_tab():
    building_id = st.session_state["selected_building"]
    anchor_ts = get_latest_data_timestamp(building_id)

    st.markdown("### Snapshot pregled")
    st.markdown(
        '<div class="muted">Prikazani podaci su zadnji upisani u bazu. Nema real-time monitoringa.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="muted">Snapshot anchor (zadnji timestamp u bazi): <b>{anchor_ts or "N/A"}</b></div>',
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.markdown("#### Prikaz mape zgrade")

    units = get_units_for_building(building_id)
    latest = get_latest_readings(building_id, anchor_ts=anchor_ts)

    for floor in sorted(units["floor"].unique(), reverse=True):
        floor_units = units[units["floor"] == floor]
        st.markdown(f"**Sprat {floor}**")

        cols = st.columns(min(len(floor_units), 6))
        for idx, (_, unit) in enumerate(floor_units.iterrows()):
            unit_id = unit["unit_id"]
            unit_num = unit["unit_number"]

            unit_data = latest[latest["unit_id"] == unit_id]

            # --- ORIGINAL STATUS LOGIC (unchanged thresholds) ---
            if unit_data.empty:
                status = "N/A"
                temp = "N/A"
                energy = "N/A"
                e_val = 0.0
                dot_class = "dot-na"
            else:
                row = unit_data.iloc[0]
                e_val = float(row.get("energy", 0) or 0.0)
                temp = f"{float(row.get('temp_internal')):.1f} C" if pd.notna(row.get("temp_internal")) else "N/A"
                energy = f"{e_val:.2f} kWh" if pd.notna(row.get("energy")) else "N/A"

                if e_val > 1.5:
                    status = "Visoka"
                    dot_class = "dot-bad"
                elif e_val > 0.8:
                    status = "Srednja"
                    dot_class = "dot-warn"
                else:
                    status = "Normalna"
                    dot_class = "dot-ok"

            with cols[idx % len(cols)]:
                st.markdown(
                    f"""
                    <div class="unit-card">
                      <div class="unit-title"><span class="status-dot {dot_class}"></span>Stan {unit_num}</div>
                      <div class="unit-meta">Status: {status}</div>
                      <div class="unit-meta">Temperatura: {temp}</div>
                      <div class="unit-meta">Energija: {energy}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    st.markdown("#### Aktivni alerti (zadnjih 24h u odnosu na snapshot)")
    alerts = get_active_alerts(building_id, hours=24)

    if alerts.empty:
        st.success("Nema alertova u snapshot prozoru.")
    else:
        st.dataframe(
            alerts[["timestamp", "unit_id", "anomaly_type", "severity", "value"]],
            use_container_width=True,
            hide_index=True,
            height=260,
        )

    st.markdown("---")

    st.markdown("#### Potrošnja (zadnjih 24h u odnosu na snapshot)")
    consumption = get_consumption_timeseries(building_id, hours=24)

    if not consumption.empty:
        fig = px.line(
            consumption,
            x="timestamp",
            y="total_kwh",
            title="Ukupna potrošnja po intervalu (snapshot prozor)",
            labels={"total_kwh": "kWh", "timestamp": "Vrijeme"},
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nema podataka o potrošnji u izabranom snapshot prozoru.")


# ============================================
# TAB 2: ANALYTICS (design-only changes)
# ============================================
def render_analytics_tab():
    st.markdown("### Analitika (offline / historijski podaci)")

    building_id = st.session_state["selected_building"]

    col1, col2 = st.columns([1, 3])
    with col1:
        days = st.selectbox("Vremenski period (vezan za snapshot)", [1, 7, 30], index=1)

    st.markdown(f"#### Trend potrošnje ({days} dana)")
    consumption = get_consumption_timeseries(building_id, hours=days * 24)

    if not consumption.empty:
        consumption = consumption.copy()
        consumption.set_index("timestamp", inplace=True)
        daily = consumption.resample("D").sum().reset_index()

        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily["timestamp"], y=daily["total_kwh"], name="Potrošnja"))

        avg = float(daily["total_kwh"].mean())
        fig.add_hline(y=avg, line_dash="dash", annotation_text=f"Prosjek: {avg:.1f} kWh")

        fig.update_layout(
            title=f"Dnevna potrošnja ({days} dana) [snapshot prozor]",
            xaxis_title="Datum",
            yaxis_title="kWh",
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nema podataka za izabrani period (vezano za snapshot).")


# ============================================
# TAB 3: CONTROL (design-only changes)
# ============================================
def render_control_tab():
    st.markdown("### Kontrola")
    st.info("Learning mode: sistem samo loguje odluke (ne šalje komande).")

    building_id = st.session_state["selected_building"]
    units = get_units_for_building(building_id)

    st.markdown("#### Kontrole po stanu (stub)")

    for _, unit in units.head(3).iterrows():
        with st.expander(f"Stan {unit['unit_number']} (Sprat {unit['floor']})"):
            col1, col2 = st.columns(2)
            with col1:
                st.slider("Ciljna temperatura", 17, 24, 21, key=f"temp_{unit['unit_id']}")
            with col2:
                st.selectbox("Akcija", ["Maintain", "Reduce", "Setback"], key=f"action_{unit['unit_id']}")
            st.button("Primijeni", disabled=True, key=f"apply_{unit['unit_id']}")
            st.caption("Onemogućeno u Learning modu (offline).")


# ============================================
# TAB 4: SYSTEM (design-only changes)
# ============================================
def render_system_tab():
    st.markdown("### Sistem (iz logova u bazi)")

    building_id = st.session_state["selected_building"]
    validation = get_validation_status(building_id)

    if validation is None:
        if table_exists("system_validation_log"):
            st.warning("Tabela system_validation_log postoji, ali nema redova za ovu zgradu.")
        else:
            st.warning("Tabela system_validation_log ne postoji u ovoj bazi.")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Status", str(validation["status"]).upper())
    with col2:
        st.metric(
            "Pouzdanost",
            f"{float(validation['model_confidence_avg']):.2f}" if validation["model_confidence_avg"] is not None else "N/A",
        )
    with col3:
        st.metric(
            "Pokrivenost",
            f"{float(validation['coverage']):.1%}" if validation["coverage"] is not None else "N/A",
        )
    with col4:
        blocked = int(validation["blocked_units_count"] or 0)
        st.metric("Blokirani stanovi", blocked)

    st.markdown("---")
    st.markdown("#### Nedavne odluke (zadnjih 24h u odnosu na snapshot)")
    decisions = get_recent_decisions(building_id, hours=24)

    if not decisions.empty:
        st.dataframe(
            decisions[["timestamp", "unit_id", "action", "approved", "confidence"]],
            use_container_width=True,
            hide_index=True,
            height=260,
        )
    else:
        st.info("Nema nedavnih odluka u snapshot prozoru.")


# ============================================
# MAIN APP (no functional changes)
# ============================================
def main():
    if "selected_building" not in st.session_state:
        buildings = get_buildings()
        if not buildings.empty:
            st.session_state["selected_building"] = buildings.iloc[0]["building_id"]

    render_top_bar()

    tab1, tab2, tab3, tab4 = st.tabs(["Snapshot pregled", "Analitika", "Kontrola", "Sistem"])
    with tab1:
        render_overview_tab()
    with tab2:
        render_analytics_tab()
    with tab3:
        render_control_tab()
    with tab4:
        render_system_tab()


if __name__ == "__main__":
    main()
