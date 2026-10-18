"""SQLite persistente. Inicializar no borra los datos existentes."""
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_NAME = os.getenv(
    "DB_PATH", str(Path(__file__).with_name("water_management.db"))
)


@contextmanager
def connection():
    conn = sqlite3.connect(DB_NAME, timeout=15)
    conn.row_factory = sqlite3.Row

    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db():
    Path(DB_NAME).parent.mkdir(parents=True, exist_ok=True)

    with connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS operators (
                id TEXT PRIMARY KEY,
                name TEXT
            );

            CREATE TABLE IF NOT EXISTS watering_stations (
                id TEXT PRIMARY KEY,
                location TEXT,
                status TEXT DEFAULT 'OFFLINE',
                current_flow REAL DEFAULT 0,
                accumulated_volume REAL DEFAULT 0,
                current_operator TEXT,
                last_health_check TEXT,
                blocked INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS irrigation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ws_id TEXT,
                operator_id TEXT,
                start_time TEXT,
                end_time TEXT,
                total_volume REAL
            );
        """)

        # Permite utilizar una BD anterior sin borrar sus tablas.
        columns = {
            r["name"]
            for r in conn.execute("PRAGMA table_info(watering_stations)")
        }

        if "blocked" not in columns:
            conn.execute(
                "ALTER TABLE watering_stations "
                "ADD COLUMN blocked INTEGER DEFAULT 0"
            )

        conn.executemany(
            "INSERT OR IGNORE INTO operators(id,name) VALUES (?,?)",
            [
                ("FO-01", "Operario 1"),
                ("FO-02", "Operario 2"),
                ("FO-03", "Operario 3"),
            ],
        )

        conn.executemany(
            "INSERT OR IGNORE INTO watering_stations(id,location) VALUES (?,?)",
            [
                ("WS-01", "Parque Central"),
                ("WS-02", "Jardines del Rio"),
                ("WS-03", "Plaza Mayor"),
            ],
        )


if __name__ == "__main__":
    init_db()
    print("Base de datos preparada")