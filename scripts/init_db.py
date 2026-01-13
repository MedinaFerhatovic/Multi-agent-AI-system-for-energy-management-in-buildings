import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"
SQL_PATH = BASE_DIR / "db" / "init_db.sql"

def init_database():
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"No SQL file: {SQL_PATH}")

    sql_script = SQL_PATH.read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(sql_script)
        conn.commit()
        print("Tabeles created in database!")
        print(f"Database location: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()
