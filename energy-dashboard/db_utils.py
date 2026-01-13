from __future__ import annotations

from pathlib import Path
import sqlite3
import streamlit as st
from datetime import datetime, timedelta

DB_NAME = "smartbuilding.db"


def _find_project_root(start: Path) -> Path | None:
    """
    Nađe root projekta tako što ide prema gore i traži:
    - folder 'db' i fajl 'init_db.sql' (najbolji marker kod tebe)
    - ili README.md kao fallback
    """
    cur = start.resolve()
    for parent in [cur, *cur.parents]:
        db_dir = parent / "db"
        if (db_dir / "init_db.sql").exists() and (db_dir / DB_NAME).exists():
            return parent
        # fallback marker
        if (parent / "README.md").exists() and db_dir.exists():
            if (db_dir / DB_NAME).exists():
                return parent
    return None


def find_db_path() -> Path:
    """
    Nađe root/db/smartbuilding.db bez obzira odakle se pokreće Streamlit
    (dashboard.py, pages/*.py, i working directory).
    """
    here = Path(__file__).resolve()
    root = _find_project_root(here.parent)
    if root:
        db_path = (root / "db" / DB_NAME).resolve()
        if db_path.exists():
            return db_path

    # fallback: probaj relativno od current working dir
    cwd_candidate = (Path.cwd() / "db" / DB_NAME).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    # fallback: probaj relativno "db/smartbuilding.db"
    rel_candidate = (Path("db") / DB_NAME).resolve()
    if rel_candidate.exists():
        return rel_candidate

    raise FileNotFoundError(
        f"Nije pronađena baza {DB_NAME}. "
        f"Provjerio sam: project-root/db, cwd/db, i relativno db/."
    )


@st.cache_resource
def get_db_connection() -> sqlite3.Connection:
    """
    OFFLINE / SNAPSHOT: veza na SQLite bazu. Nema real-time stream-a.
    """
    db_path = find_db_path()

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Bitno za tvoju šemu (foreign keys)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Stabilnije ponašanje u Streamlitu (često re-run)
    conn.execute("PRAGMA busy_timeout = 5000;")  # 5s

    # WAL samo ako već koristiš; ako ne koristiš, ne smeta ali nije nužno
    conn.execute("PRAGMA journal_mode = WAL;")

    return conn


# ----------------------------
# Snapshot helpers (NO real-time)
# ----------------------------
def get_snapshot_anchor_ts(conn: sqlite3.Connection, building_id: str) -> str | None:
    """
    Vraća zadnji timestamp u DB za tu zgradu.
    Ovo je 'anchor' za sve prikaze (offline snapshot).
    """
    row = conn.execute(
        """
        SELECT MAX(timestamp) AS ts
        FROM sensor_readings
        WHERE building_id = ? AND quality_flag='ok'
        """,
        (building_id,),
    ).fetchone()

    if row is None or row["ts"] is None:
        return None
    return str(row["ts"])


def get_cutoff_ts_from_anchor(anchor_ts: str, hours: int) -> str:
    """
    Cutoff u odnosu na anchor_ts (ne u odnosu na 'sad').
    Radi sa 'YYYY-MM-DD HH:MM:SS' i 'YYYY-MM-DDTHH:MM:SSZ'
    """
    s = anchor_ts.replace("Z", "").replace("T", " ")
    dt = datetime.fromisoformat(s)
    cutoff = dt - timedelta(hours=hours)
    return cutoff.isoformat(sep=" ")
