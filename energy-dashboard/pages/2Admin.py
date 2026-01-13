"""
Admin i Debug stranica - sistemska dijagnostika i otklanjanje problema
Osvježenje dizajna (bez promjena funkcionalnosti)
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json

st.set_page_config(page_title="Admin panel", page_icon="", layout="wide")

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
.kicker { color: rgba(49, 51, 63, 0.70); font-size: 0.92rem; margin-top: -0.25rem; }
.muted { color: rgba(49, 51, 63, 0.65); font-size: 0.95rem; line-height: 1.35; }

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

/* Metric cards wrapper */
.metric-card {
  border: 1px solid rgba(49, 51, 63, 0.12);
  border-radius: 16px;
  padding: 12px 12px 10px 12px;
  background: #ffffff;
  box-shadow: 0 1px 10px rgba(0,0,0,0.05);
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

/* Expander */
div[data-testid="stExpander"] > div { border-radius: 16px; }
</style>
""",
    unsafe_allow_html=True,
)

# ============================================
# DATABASE CONNECTION (unchanged)
# ============================================
from db_utils import get_db_connection, find_db_path

conn = get_db_connection()
DB_PATH = find_db_path()


def table_exists(table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


def safe_read_sql(query: str, params=()):
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"SQL greška: {e}")
        return pd.DataFrame()


# ============================================
# DIAGNOSTIC QUERIES (unchanged)
# ============================================
def get_table_info(table_name):
    query = f"PRAGMA table_info({table_name})"
    return pd.read_sql_query(query, conn)


def get_table_count(table_name):
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0] if result else 0
    except Exception:
        return None


def get_db_size():
    if DB_PATH.exists():
        size_bytes = DB_PATH.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        return f"{size_mb:.2f} MB"
    return "N/A"


def get_data_quality_stats(building_id):
    query = """
    SELECT 
        sensor_type,
        quality_flag,
        COUNT(*) as count,
        MIN(timestamp) as first_reading,
        MAX(timestamp) as last_reading
    FROM sensor_readings
    WHERE building_id = ?
    GROUP BY sensor_type, quality_flag
    ORDER BY sensor_type, quality_flag
    """
    return pd.read_sql_query(query, conn, params=(building_id,))


def get_model_registry():
    query = """
    SELECT 
        model_id,
        model_scope,
        model_task,
        model_type,
        feature_version,
        trained_at,
        is_active,
        metrics_json
    FROM model_registry
    ORDER BY trained_at DESC
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df["metrics"] = df["metrics_json"].apply(lambda x: json.loads(x) if x else {})
    return df


def get_pipeline_progress():
    query = """
    SELECT 
        pipeline_name,
        building_id,
        current_anchor_ts,
        updated_at
    FROM pipeline_progress
    ORDER BY updated_at DESC
    """
    return pd.read_sql_query(query, conn)


def get_recent_errors():
    query = """
    SELECT 
        'anomaly' as type,
        timestamp,
        building_id,
        unit_id,
        anomaly_type as detail,
        severity
    FROM anomalies_log
    WHERE severity IN ('critical', 'high')
    AND timestamp >= datetime('now', '-24 hours')
    
    UNION ALL
    
    SELECT 
        'decision_blocked' as type,
        timestamp,
        building_id,
        unit_id,
        action as detail,
        'blocked' as severity
    FROM decisions_log
    WHERE approved = 0
    AND timestamp >= datetime('now', '-24 hours')
    
    ORDER BY timestamp DESC
    LIMIT 50
    """
    return pd.read_sql_query(query, conn)


# ============================================
# MAIN PAGE
# ============================================
def main():
    st.markdown("## Admin i debug panel")
    st.markdown(
        '<div class="kicker">Dijagnostički alat za otklanjanje problema u sistemu (samo čitanje).</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="panel"><div class="muted"><span class="badge">Napomena</span>'
        '<div style="margin-top:8px;">Ova stranica prikazuje dijagnostiku u režimu samo-čitanje. Za izmjene koristite direktan pristup bazi.</div>'
        "</div></div>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # ============================================
    # DATABASE HEALTH
    # ============================================
    st.markdown('<div class="section-title">Status baze</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Osnovne informacije o konekciji i zadnjem upisu.</div>',
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        status = "Povezano" if DB_PATH.exists() else "Nije pronađeno"
        st.metric("Konekcija", status)
        st.caption(f"DB: {DB_PATH}")
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.metric("Veličina baze", get_db_size())
        st.markdown("</div>", unsafe_allow_html=True)

    with col3:
        if table_exists("sensor_readings"):
            result = safe_read_sql("SELECT MAX(timestamp) as last FROM sensor_readings")
            last_ts = result["last"].iloc[0] if not result.empty else None
            st.metric("Zadnji upis (DB)", last_ts or "N/A")
        else:
            st.metric("Zadnji upis (DB)", "N/A", "sensor_readings nedostaje")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ============================================
    # TABLE OVERVIEW
    # ============================================
    st.markdown('<div class="section-title">Pregled tabela</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Brzi pregled dostupnosti i broja redova po tabelama.</div>',
        unsafe_allow_html=True,
    )

    tables = [
        "buildings",
        "units",
        "sensors",
        "sensor_readings",
        "external_weather",
        "unit_features_daily",
        "clusters",
        "predictions",
        "optimization_plans",
        "decisions_log",
        "anomalies_log",
        "model_registry",
        "pipeline_progress",
        "system_validation_log",
    ]

    table_stats = []
    for table in tables:
        count = get_table_count(table)
        table_stats.append(
            {
                "Tabela": table,
                "Broj redova": count if count is not None else "Greška",
                "Status": "OK" if count is not None and count > 0 else "Provjeriti",
            }
        )

    df_stats = pd.DataFrame(table_stats)

    col1, col2 = st.columns(2)
    mid = len(df_stats) // 2
    with col1:
        st.dataframe(df_stats.iloc[:mid], use_container_width=True, hide_index=True)
    with col2:
        st.dataframe(df_stats.iloc[mid:], use_container_width=True, hide_index=True)

    st.markdown("---")

    # ============================================
    # DATA QUALITY
    # ============================================
    st.markdown('<div class="section-title">Kvalitet podataka</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Distribucija quality_flag po tipu senzora za izabranu zgradu.</div>',
        unsafe_allow_html=True,
    )

    buildings_query = "SELECT building_id, name FROM buildings"
    buildings = pd.read_sql_query(buildings_query, conn)

    if buildings.empty:
        st.warning("Nema zgrada u bazi.")
        st.stop()

    building_id = st.selectbox(
        "Izaberite zgradu",
        buildings["building_id"].tolist(),
        format_func=lambda x: f"{x} - {buildings[buildings['building_id'] == x]['name'].iloc[0]}",
    )

    quality_stats = get_data_quality_stats(building_id)

    if not quality_stats.empty:
        for sensor_type in quality_stats["sensor_type"].unique():
            with st.expander(f"{sensor_type.upper()}"):
                sensor_df = quality_stats[quality_stats["sensor_type"] == sensor_type]

                col1, col2, col3 = st.columns(3)

                ok_count = sensor_df[sensor_df["quality_flag"] == "ok"]["count"].sum()
                total_count = sensor_df["count"].sum()
                bad_count = total_count - ok_count

                with col1:
                    st.metric("OK očitanja", f"{ok_count:,}")
                with col2:
                    st.metric("Loša očitanja", f"{bad_count:,}")
                with col3:
                    quality_pct = (ok_count / total_count * 100) if total_count > 0 else 0
                    st.metric("Kvalitet", f"{quality_pct:.1f}%")

                st.dataframe(sensor_df, use_container_width=True, hide_index=True)
    else:
        st.warning("Nema podataka o kvalitetu.")

    st.markdown("---")

    # ============================================
    # MODEL STATUS
    # ============================================
    st.markdown('<div class="section-title">ML modeli</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Pregled registrovanih i aktivnih modela.</div>',
        unsafe_allow_html=True,
    )

    models = get_model_registry()

    if not models.empty:
        active_models = models[models["is_active"] == 1]

        if not active_models.empty:
            st.success(f"Aktivnih modela: {len(active_models)}")

            for _, model in active_models.iterrows():
                with st.expander(f"{model['model_id']}"):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.markdown(f"**Opseg (scope):** {model['model_scope']}")
                        st.markdown(f"**Zadatak (task):** {model['model_task']}")

                    with col2:
                        st.markdown(f"**Tip:** {model['model_type']}")
                        st.markdown(f"**Feature verzija:** v{model['feature_version']}")

                    with col3:
                        st.markdown(f"**Treniran:** {model['trained_at']}")

                    st.markdown("#### Metričke performansi")
                    metrics = model["metrics"]

                    if metrics:
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            test_mae = metrics.get("test_mae", "N/A")
                            st.metric("Test MAE", f"{test_mae:.4f}" if test_mae != "N/A" else "N/A")
                        with c2:
                            test_rmse = metrics.get("test_rmse", "N/A")
                            st.metric("Test RMSE", f"{test_rmse:.4f}" if test_rmse != "N/A" else "N/A")
                        with c3:
                            test_r2 = metrics.get("test_r2", "N/A")
                            st.metric("Test R²", f"{test_r2:.4f}" if test_r2 != "N/A" else "N/A")
        else:
            st.warning("Nema aktivnih modela.")

        st.markdown("### Svi modeli")
        st.dataframe(
            models[["model_id", "model_scope", "model_task", "trained_at", "is_active"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.error("Nema registrovanih modela.")

    st.markdown("---")

    # ============================================
    # PIPELINE STATUS
    # ============================================
    st.markdown('<div class="section-title">Workflow pipeline</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Stanje izvršenja pipeline-a iz pipeline_progress.</div>',
        unsafe_allow_html=True,
    )

    progress = get_pipeline_progress()

    if not progress.empty:
        st.dataframe(progress, use_container_width=True, hide_index=True)

        for _, row in progress.iterrows():
            updated = datetime.fromisoformat(row["updated_at"])
            age = datetime.now() - updated
            if age > timedelta(hours=2):
                st.warning(
                    f"Pipeline '{row['pipeline_name']}' za '{row['building_id']}' nije osvježen {age}"
                )
    else:
        st.info("Nema podataka o napretku pipeline-a.")

    st.markdown("---")

    # ============================================
    # RECENT ERRORS
    # ============================================
    st.markdown('<div class="section-title">Nedavni problemi (zadnja 24 sata)</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Kritične anomalije i blokirane odluke.</div>',
        unsafe_allow_html=True,
    )

    errors = get_recent_errors()

    if not errors.empty:
        col1, col2 = st.columns(2)

        with col1:
            anomaly_count = len(errors[errors["type"] == "anomaly"])
            st.metric("Kritične anomalije", anomaly_count)

        with col2:
            blocked_count = len(errors[errors["type"] == "decision_blocked"])
            st.metric("Blokirane odluke", blocked_count)

        st.dataframe(errors, use_container_width=True, hide_index=True)
    else:
        st.success("Nema nedavnih problema.")

    st.markdown("---")

    # ============================================
    # SQL PLAYGROUND
    # ============================================
    st.markdown('<div class="section-title">SQL alat</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Režim samo-čitanje: dozvoljeni su samo SELECT upiti.</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="panel"><span class="badge">Samo čitanje</span>'
        '<div class="muted" style="margin-top:8px;">Dozvoljeni su samo SELECT upiti.</div></div>',
        unsafe_allow_html=True,
    )

    query_input = st.text_area(
        "SQL upit",
        value="SELECT * FROM buildings LIMIT 10",
        height=150,
    )

    if st.button("Pokreni upit"):
        if not query_input.strip().upper().startswith("SELECT"):
            st.error("Dozvoljeni su samo SELECT upiti.")
        else:
            try:
                result = pd.read_sql_query(query_input, conn)
                st.success(f"Upit izvršen. Broj vraćenih redova: {len(result)}")
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(f"Greška u upitu: {str(e)}")

    st.markdown("---")

    # ============================================
    # CACHE MANAGEMENT
    # ============================================
    st.markdown('<div class="section-title">Upravljanje kešom</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-sub">Osvježavanje keša i stranice tokom razvoja.</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Očisti sve keševe"):
            st.cache_data.clear()
            st.success("Keš je očišćen.")
            st.rerun()

    with col2:
        if st.button("Osvježi stranicu"):
            st.rerun()


if __name__ == "__main__":
    main()
