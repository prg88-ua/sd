"""SQLite persistente. Inicializar no borra los datos existentes."""
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

DB_NAME = os.getenv(
    "DB_PATH", str(Path(__file__).with_name("water_management.db"))
)
_local = threading.local()


@contextmanager
def connection():
    # Las llamadas anidadas de un mismo hilo comparten UNA transacción.
    # Así, pedido + respuesta pendiente + posición Kafka se confirman juntos.
    if getattr(_local, "conn", None) is not None:
        yield _local.conn
        return
    conn = sqlite3.connect(DB_NAME, timeout=15)
    conn.row_factory = sqlite3.Row
    _local.conn = conn

    try:
        with conn:
            yield conn
    finally:
        _local.conn = None
        conn.close()


def init_db(seed_demo=True):
    """Crea/migra el esquema; las tres identidades de ejemplo son opcionales."""
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

            CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS central_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kafka_offsets (
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                next_offset INTEGER NOT NULL,
                PRIMARY KEY (topic, partition_id)
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

        if "last_request_id" not in columns:
            conn.execute("ALTER TABLE watering_stations ADD COLUMN last_request_id TEXT")
<<<<<<< HEAD

        history_columns = {
            r["name"] for r in conn.execute("PRAGMA table_info(irrigation_history)")
        }
        if "request_id" not in history_columns:
            conn.execute("ALTER TABLE irrigation_history ADD COLUMN request_id TEXT")
        # Los registros antiguos, sin identificador, se conservan sin modificarlos.
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS history_request ON irrigation_history(request_id)")

        conn.executemany(
            "INSERT OR IGNORE INTO operators(id,name) VALUES (?,?)",
            [
                ("FO-01", "Operario 1"),
                ("FO-02", "Operario 2"),
                ("FO-03", "Operario 3"),
            ],
        )
=======
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

        history_columns = {
            r["name"] for r in conn.execute("PRAGMA table_info(irrigation_history)")
        }
        if "request_id" not in history_columns:
            conn.execute("ALTER TABLE irrigation_history ADD COLUMN request_id TEXT")
        # Los registros antiguos, sin identificador, se conservan sin modificarlos.
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS history_request ON irrigation_history(request_id)")

        if seed_demo:
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
