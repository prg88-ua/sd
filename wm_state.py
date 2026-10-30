"""Estado local fuera de la imagen Docker; WM_STATE_DIR permite montar un volumen."""
import json
import os
from pathlib import Path
import sqlite3
import re


def state_dir():
    directory = Path(os.getenv("WM_STATE_DIR", str(Path(__file__).parent / ".wm_state")))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("El identificador solo admite letras, números, guion y guion bajo")
    return value


def atomic_json(path, data):
    """Primero escribir y sincronizar; después sustituir el archivo completo."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(data, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class EngineJournal:
    """Un registro por riego: no se borra el anterior al empezar el siguiente."""

    def __init__(self, station):
        self.station = safe_id(station)
        self.path = state_dir() / f"engine_{station}.db"
        with self.connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, status TEXT, payload TEXT)")

    def connect(self):
        # Cada hilo usa su propia conexión; SQLite serializa las escrituras.
        from contextlib import closing, contextmanager

        @contextmanager
        def session():
            with closing(sqlite3.connect(self.path, timeout=15)) as conn:
                with conn:
                    yield conn
        return session()

    def save(self, run, status):
        with self.connect() as conn:
            conn.execute("INSERT INTO runs VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload",
                         (run["id"], status, json.dumps(run)))

    def get(self, request_id):
        with self.connect() as conn:
            row = conn.execute("SELECT status,payload FROM runs WHERE id=?", (request_id,)).fetchone()
        return (row[0], json.loads(row[1])) if row else None

    def recover(self):
        """Una caída cierra el riego; nunca se vuelve a abrir la válvula al reiniciar."""
        legacy = Path(f"engine_state_{self.station}.json")
        if legacy.exists():
            data = json.loads(legacy.read_text(encoding="utf-8"))
            if not self.get(data["id"]):
                self.save(data, "IN_PROGRESS")
            legacy.unlink()  # Solo tras confirmar la importación a SQLite.
        with self.connect() as conn:
            rows = conn.execute("SELECT status,payload FROM runs WHERE status != 'DELIVERED'").fetchall()
        pending = []
        for status, payload in rows:
            run = json.loads(payload)
            if status == "IN_PROGRESS":
                run = dict(run, type="END", ws=self.station, flow=0,
                           reason="INTERRUMPIDO_POR_CAIDA_ENGINE")
                self.save(run, "FINAL_PENDING")
            pending.append(run)
        return pending

    def delivered(self, request_id):
        # Se conserva el ID para no ejecutar otra vez un START repetido.
        with self.connect() as conn:
            conn.execute("UPDATE runs SET status='DELIVERED' WHERE id=? AND status='FINAL_PENDING'", (request_id,))
