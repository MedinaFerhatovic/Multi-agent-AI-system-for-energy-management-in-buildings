from __future__ import annotations

from pathlib import Path
import sqlite3
import streamlit as st
from datetime import datetime, timedelta

DB_NAME = "smartbuilding.db"


def _find_project_root(start: Path) -> Path | None:
    cur = start.resolve()
    for parent in [cur, *cur.parents]:
        db_dir = parent / "db"
        if (db_dir / "init_db.sql").exists() and (db_dir / DB_NAME).exists():
            return parent
        if (parent / "README.md").exists() and db_dir.exists():
            if (db_dir / DB_NAME).exists():
                return parent
    return None


def find_db_path() -> Path:
    here = Path(__file__).resolve()
    root = _find_project_root(here.parent)
    if root:
        db_path = (root / "db" / DB_NAME).resolve()
        if db_path.exists():
            return db_path

    cwd_candidate = (Path.cwd() / "db" / DB_NAME).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    rel_candidate = (Path("db") / DB_NAME).resolve()
    if rel_candidate.exists():
        return rel_candidate

    raise FileNotFoundError(
        f"Nije pronađena baza {DB_NAME}. "
        f"Provjerio sam: project-root/db, cwd/db, i relativno db/."
    )


@st.cache_resource
def get_db_connection() -> sqlite3.Connection:
    db_path = find_db_path()

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute("PRAGMA busy_timeout = 5000;")  

    conn.execute("PRAGMA journal_mode = WAL;")

    return conn


def get_snapshot_anchor_ts(conn: sqlite3.Connection, building_id: str) -> str | None:
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
    s = anchor_ts.replace("Z", "").replace("T", " ")
    dt = datetime.fromisoformat(s)
    cutoff = dt - timedelta(hours=hours)
    return cutoff.isoformat(sep=" ")
