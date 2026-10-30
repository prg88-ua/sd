"""Regresiones del registro de eventos y de su lectura desde el panel."""
import concurrent.futures
import unittest

from WM_Central.logs import CentralLogs
from WM_Central.WM_Central import create_app
from tests.test_recovery import IsolatedTest


class LogBufferTests(unittest.TestCase):
    def test_ring_is_bounded_and_reports_a_gap_to_a_slow_reader(self):
        logs = CentralLogs(capacity=3)
        for number in range(6):
            logs.append(f"Evento {number}")
        result = logs.read(1, logs.session)
        self.assertEqual([entry["id"] for entry in result["entries"]], [4, 5, 6])
        self.assertEqual(result["last_id"], 6)
        self.assertTrue(result["truncated"])
        self.assertFalse(logs.read(6, logs.session)["entries"])

    def test_restarting_central_resets_the_cursor_even_if_ids_have_caught_up(self):
        previous = CentralLogs()
        current = CentralLogs()
        for number in range(5):
            current.append(f"Evento nuevo {number}")
        result = current.read(2, previous.session)
        self.assertTrue(result["reset"])
        self.assertEqual(len(result["entries"]), 5)

    def test_concurrent_producers_have_unique_ordered_ids(self):
        logs = CentralLogs()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(logs.append, [f"Evento {number}" for number in range(200)]))
        self.assertEqual([entry["id"] for entry in logs.read()["entries"]], list(range(1, 201)))


class DashboardLogTests(IsolatedTest):
    def test_http_reads_only_new_logs_without_logging_its_own_polls(self):
        central = self.central()
        client = create_app(central).test_client()
        initial = client.get("/api/logs")
        self.assertEqual(initial.status_code, 200)
        self.assertEqual(initial.headers["Cache-Control"], "no-store")
        cursor = initial.json["last_id"]
        session = initial.json["session"]
        central.log("[KAFKA] Conexión perdida", "ERROR")
        result = client.get(f"/api/logs?after={cursor}&session={session}").json
        self.assertEqual(len(result["entries"]), 1)
        self.assertEqual(result["entries"][0]["level"], "ERROR")
        self.assertEqual(result["entries"][0]["message"], "[KAFKA] Conexión perdida")
        second = client.get(f"/api/logs?after={result['last_id']}&session={session}").json
        self.assertEqual(second["entries"], [])

    def test_repeated_healthy_updates_do_not_fill_the_terminal(self):
        central = self.central()
        ws = central.stations["WS-01"]
        central.save(ws)
        initial = central.logs.read()["last_id"]
        central.save(ws)
        central.save(ws)
        self.assertFalse(central.logs.read(initial)["entries"])
        ws["health"] = "KO"
        central.save(ws)
        central.save(ws)
        lines = central.logs.read(initial)["entries"]
        self.assertEqual(len(lines), 1)
        self.assertIn("AVAILABLE -> LEAK", lines[0]["message"])

    def test_bad_cursors_are_rejected(self):
        client = create_app(self.central()).test_client()
        for cursor in ("-1", "abc", "1.5"):
            with self.subTest(cursor=cursor):
                self.assertEqual(client.get(f"/api/logs?after={cursor}").status_code, 400)


if __name__ == "__main__":
    unittest.main()
