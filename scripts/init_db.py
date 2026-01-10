import sqlite3
from pathlib import Path

# Putanje
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"
SQL_PATH = BASE_DIR / "db" / "init_db.sql"

def init_database():
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"Ne mogu naći SQL fajl: {SQL_PATH}")

    # Učitaj SQL skriptu
    sql_script = SQL_PATH.read_text(encoding="utf-8")

    # Spoji se na SQLite bazu i izvrši SQL
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(sql_script)
        conn.commit()
        print("Tabele su uspješno kreirane u bazi!")
        print(f"Lokacija baze: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()
