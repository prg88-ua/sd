"""Alta real en SQLite y API Flask, sin arrancar broker ni procesos externos."""
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import WM_Central.database as database
from WM_Central.WM_Central import Central, create_app


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP"))
        self.addCleanup(self.tmp.cleanup)
        db_patch = patch.object(database, "DB_NAME", str(Path(self.tmp.name) / "central.db"))
        db_patch.start()
        self.addCleanup(db_patch.stop)
        database.init_db(seed_demo=False)
        self.central = Central()
        self.app = create_app(self.central)
        self.client = self.app.test_client()
        self.payload = {
            "operators": [{"id": "FO-20", "name": "Operario 20"}],
            "stations": [{"id": "WS-20", "location": "Parque 20"}],
        }

    def post(self, payload=None, headers=None):
        return self.client.post(
            "/api/catalog",
            json=self.payload if payload is None else payload,
            headers={} if headers is None else headers,
        )

    def counts(self):
        with database.connection() as conn:
            return tuple(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                         for table in ("operators", "watering_stations", "irrigation_history"))

    def test_schema_only_does_not_add_demo_rows(self):
        self.assertEqual(self.counts(), (0, 0, 0))
        database.init_db()
        self.assertEqual(self.counts(), (3, 3, 0))
        database.init_db(seed_demo=False)
        self.assertEqual(self.counts(), (3, 3, 0))

    def test_catalog_updates_cache_and_survives_restart(self):
        response = self.post()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["created"], {"operators": 1, "stations": 1})
        self.assertEqual(self.counts(), (1, 1, 0))
        self.assertIn("WS-20", self.central.stations)
        self.assertEqual(self.central.status(self.central.stations["WS-20"]), "OFFLINE")
        restarted = Central()
        self.assertIn("WS-20", restarted.stations)
        self.central.restore_requests()  # Nueva identidad disponible en la caché de recuperación.

    def test_repeating_catalog_does_not_duplicate_rows(self):
        self.post()
        response = self.post()
        self.assertEqual(response.json["created"], {"operators": 0, "stations": 0})
        self.assertEqual(self.counts(), (1, 1, 0))

    def test_existing_station_keeps_live_state_volume_and_history(self):
        self.post()
        station = self.central.stations["WS-20"]
        station.update(live=True, health="OK", seen=time.monotonic(),
                       accumulated_volume=45.5, blocked=1, current_operator="FO-20",
                       current_flow=2.2, last_request_id="R-20")
        self.central.save(station)
        with database.connection() as conn:
            conn.execute(
                "INSERT INTO irrigation_history(ws_id,operator_id,total_volume,request_id) VALUES (?,?,?,?)",
                ("WS-20", "FO-20", 45.5, "R-20"),
            )
        before = dict(station)
        altered = {
            "operators": [{"id": "FO-20", "name": "Otro nombre"}],
            "stations": [{"id": "WS-20", "location": "Otro parque"}],
        }
        self.assertEqual(self.post(altered).status_code, 200)
        self.assertEqual(station, before)
        with database.connection() as conn:
            stored = dict(conn.execute("SELECT * FROM watering_stations").fetchone())
            self.assertEqual(stored["accumulated_volume"], 45.5)
            self.assertEqual(stored["blocked"], 1)
            self.assertEqual(stored["location"], "Parque 20")
            self.assertEqual(conn.execute("SELECT name FROM operators").fetchone()[0], "Operario 20")
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_new_operator_can_consult_new_station_without_restart(self):
        self.post()
        self.central.handle_request({"type": "LIST", "id": "L20", "operator": "FO-20"})
        with database.connection() as conn:
            result = json.loads(conn.execute("SELECT payload FROM central_outbox").fetchone()[0])
        self.assertEqual(result["type"], "STATIONS")
        self.assertEqual(result["stations"][0]["id"], "WS-20")

    def test_invalid_last_entry_writes_nothing(self):
        self.payload["stations"].append({"id": "bad/id", "location": "Inválido"})
        self.assertEqual(self.post().status_code, 400)
        self.assertEqual(self.counts(), (0, 0, 0))
        self.assertEqual(self.central.stations, {})

    def test_database_failure_rolls_back_catalog_before_publishing_cache(self):
        with database.connection() as conn:
            conn.execute("""CREATE TRIGGER reject_station BEFORE INSERT ON watering_stations
                            WHEN NEW.id='WS-99' BEGIN SELECT RAISE(ABORT,'fallo simulado'); END""")
        self.payload["stations"].append({"id": "WS-99", "location": "Parque"})
        with self.assertRaises(sqlite3.IntegrityError):
            self.central.register_catalog(self.payload)
        self.assertEqual(self.counts(), (0, 0, 0))
        self.assertEqual(self.central.stations, {})

    def test_catalog_accepts_creation_without_authentication_headers(self):
        self.assertEqual(self.post(headers={}).status_code, 200)
        self.assertEqual(self.counts(), (1, 1, 0))

    def test_catalog_rejects_oversized_http_body_without_writing(self):
        response = self.client.post("/api/catalog", data="x" * (1024 * 1024 + 1),
                                    content_type="application/json")
        self.assertEqual(response.status_code, 413)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_invalid_batches_are_rejected(self):
        invalid = [
            [], {"unknown": []}, {"operators": "FO-20"},
            {"stations": [{"id": "WS-01"}]},
            {"operators": [{"id": 20, "name": "Operario"}]},
            {"operators": [{"id": "FO-20", "name": " "}]},
            {"stations": [{"id": "WS-20", "location": "a#b"}]},
            {"stations": [{"id": "WS-20", "location": "a\nb"}]},
            {"stations": [{"id": "a" * 81, "location": "Parque"}]},
            {"operators": [{"id": "FO-20", "name": "x" * 101}]},
            {"operators": [{"id": "FO-20", "name": "Operario"}] * 2},
            {"operators": [{"id": "FO-20", "name": "Operario"}] * 501},
        ]
        for payload in invalid:
            with self.subTest(payload=str(payload)[:100]):
                self.assertEqual(self.post(payload).status_code, 400)
                self.assertEqual(self.counts(), (0, 0, 0))

    def test_invalid_json_is_rejected(self):
        response = self.client.post(
            "/api/catalog", data="{bad", content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.counts(), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
