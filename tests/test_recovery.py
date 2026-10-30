"""Regresiones de resiliencia: SQLite real, broker y fallos de red simulados."""
import contextlib
import io
import json
import os
from pathlib import Path
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import WM_Central.database as database
import WM_Central.WM_Central as central_module
from WM_Central.WM_Central import Central
import WM_FO.WM_FO as fo
import WM_WS.WM_WS_E.WM_WS_E as engine
from wm_state import EngineJournal
import wm_common


class FakeConsumer:
    def __init__(self, batches=()):
        self.batches = list(batches)
        self.closed = False

    def add(self, *messages):
        records = [types.SimpleNamespace(value=json.dumps(m).encode()) for m in messages]
        self.batches.append({"partition": records})

    def poll(self, **kwargs):
        return self.batches.pop(0) if self.batches else {}

    def close(self):
        self.closed = True


class IsolatedTest(unittest.TestCase):
    def setUp(self):
        # WM_TEST_TMP permite usar una raíz escribible en entornos restringidos.
        self.tmp = tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP"))
        self.addCleanup(self.tmp.cleanup)
        self.env = patch.dict(os.environ, {"WM_STATE_DIR": self.tmp.name})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.dbpatch = patch.object(database, "DB_NAME", str(Path(self.tmp.name) / "central.db"))
        self.dbpatch.start()
        self.addCleanup(self.dbpatch.stop)
        database.init_db()
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def central(self):
        central = Central()
        central.recovering = False
        for ws in central.stations.values():
            ws.update(live=True, health="OK", seen=time.monotonic())
        return central

    def request(self, request_id="A", operator="FO-01"):
        return dict(type="REQUEST", id=request_id, ws="WS-01", operator=operator,
                    duration=30, submitted_at=time.time())

    def event(self, kind, request_id="A", volume=1.0, elapsed=4.0, **extra):
        return dict(type=kind, id=request_id, ws="WS-01", operator="FO-01",
                    volume=volume, elapsed=elapsed, flow=15 if kind == "TELEMETRY" else 0,
                    reason="COMPLETADO", **extra)

    def count(self, table):
        with database.connection() as conn:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def record(self, message, topic, offset):
        return types.SimpleNamespace(value=json.dumps(message).encode(),
                                     topic=topic, partition=0, offset=offset)


class CentralTests(IsolatedTest):
    def test_active_request_survives_restart(self):
        first = self.central()
        first.handle_request(self.request())
        first.handle_event(self.event("STARTED"))
        second = Central()
        self.assertEqual(second.active["WS-01"]["id"], "A")
        self.assertEqual(second.status(second.stations["WS-01"]), "OFFLINE")

    def test_result_survives_restart_and_list(self):
        first = self.central()
        first.handle_request(self.request())
        first.handle_event(self.event("END"))
        second = self.central()
        second.handle_request(dict(type="LIST", id="L", operator="FO-01", recover_id="A"))
        with database.connection() as conn:
            msg = json.loads(conn.execute("SELECT payload FROM central_outbox ORDER BY id DESC").fetchone()[0])
        self.assertEqual(msg["last_completed"]["volume"], 1)
        self.assertEqual(second.last_completed_by_operator["FO-01"]["id"], "A")

    def test_duplicate_end_has_single_history_row(self):
        central = self.central()
        central.handle_request(self.request())
        central.handle_event(self.event("END"))
        central.handle_event(self.event("END"))
        self.assertEqual(self.count("irrigation_history"), 1)

    def test_duplicate_request_does_not_start_twice(self):
        central = self.central()
        central.handle_request(self.request())
        central.handle_event(self.event("END"))
        central = self.central()
        central.handle_request(self.request())
        with database.connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM central_outbox WHERE topic='ws_orders'").fetchone()[0]
        self.assertEqual(count, 1)

    def test_late_end_does_not_change_another_run(self):
        central = self.central()
        central.handle_request(self.request("A"))
        central.handle_event(self.event("TELEMETRY"))
        central.finish("WS-01", "SIN_RESPUESTA_ULTIMA_MEDIDA")
        central.handle_request(self.request("B", "FO-02"))
        central.handle_event(self.event("STARTED", "B"))
        central.handle_event(self.event("TELEMETRY", "B", volume=.5, elapsed=2))
        before = dict(central.stations["WS-01"])
        central.handle_event(self.event("END", "A", volume=1.5, elapsed=6))
        after = central.stations["WS-01"]
        for key in ("current_flow", "current_operator", "accumulated_volume", "last_request_id"):
            self.assertEqual(before[key], after[key])
        self.assertEqual(central.active["WS-01"]["id"], "B")

    def test_provisional_result_is_corrected_in_same_row(self):
        central = self.central()
        central.handle_request(self.request())
        central.handle_event(self.event("TELEMETRY"))
        central.finish("WS-01", "SIN_RESPUESTA_ULTIMA_MEDIDA")
        central.handle_event(self.event("END", volume=1.5, elapsed=6))
        self.assertEqual(self.count("irrigation_history"), 1)
        self.assertTrue(central.last_completed_by_operator["FO-01"]["final"])
        self.assertEqual(central.last_completed_by_operator["FO-01"]["volume"], 1.5)

    def test_old_correction_does_not_replace_newer_result(self):
        central = self.central()
        central.handle_request(self.request("A"))
        central.finish("WS-01", "SIN_RESPUESTA_ULTIMA_MEDIDA")
        central.handle_request(self.request("B"))
        central.handle_event(self.event("END", "B"))
        central.handle_event(self.event("END", "A"))
        self.assertEqual(central.last_completed_by_operator["FO-01"]["id"], "B")

    def test_pending_events_after_central_restart_are_processed(self):
        central = self.central()
        central.process_record(self.record(self.request(), "fo_requests", 0))
        central.process_record(self.record(self.event("STARTED"), "ws_events", 0))
        central = self.central()
        central.process_record(self.record(self.event("END"), "ws_events", 1))
        self.assertEqual(central.offsets()[("ws_events", 0)], 2)
        self.assertEqual(self.count("irrigation_history"), 1)

    def test_request_and_offset_rollback_together(self):
        central = self.central()
        original = central.emit

        def crash(topic, message):
            if topic == "ws_orders":
                raise OSError("Caída simulada después de preparar PENDING")
            original(topic, message)

        with patch.object(central, "emit", side_effect=crash):
            with self.assertRaises(OSError):
                central.process_record(self.record(self.request(), "fo_requests", 0))
        restarted = self.central()
        self.assertIsNone(restarted.load_request("A"))
        self.assertEqual(restarted.offsets(), {})
        self.assertEqual(self.count("central_outbox"), 0)

    def test_outbox_survives_publish_failure(self):
        central = self.central()
        central.emit("fo_responses", dict(type="END", id="A"))
        with patch.object(central_module, "publish", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                central.flush_outbox(None)
        restarted = self.central()
        self.assertEqual(self.count("central_outbox"), 1)
        with patch.object(central_module, "publish"):
            restarted.flush_outbox(None)
        self.assertEqual(self.count("central_outbox"), 0)

    def test_expired_unregistered_request_cannot_reopen_valve(self):
        central = self.central()
        central.handle_request(dict(self.request(), submitted_at=time.time()-3600))
        self.assertFalse(central.active)
        self.assertEqual(central.load_request("A")[0], "DENIED")

    def test_invalid_measures_are_not_saved(self):
        central = self.central()
        central.handle_request(self.request())
        central.handle_event(self.event("END", volume=float("nan")))
        self.assertIn("WS-01", central.active)
        self.assertEqual(self.count("irrigation_history"), 0)

    def test_old_database_is_migrated_without_losing_history(self):
        old_path = str(Path(self.tmp.name) / "old.db")
        with contextlib.closing(sqlite3.connect(old_path)) as conn, conn:
            conn.executescript("""
                CREATE TABLE watering_stations (
                    id TEXT PRIMARY KEY, location TEXT, status TEXT DEFAULT 'OFFLINE',
                    current_flow REAL DEFAULT 0, accumulated_volume REAL DEFAULT 0,
                    current_operator TEXT, last_health_check TEXT);
                CREATE TABLE irrigation_history (
                    id INTEGER PRIMARY KEY,ws_id TEXT,operator_id TEXT,
                    start_time TEXT,end_time TEXT,total_volume REAL);
                INSERT INTO irrigation_history VALUES (1,'WS-01','FO-01','antes','después',42);
            """)
        with patch.object(database, "DB_NAME", old_path):
            database.init_db()
            self.assertEqual(self.count("irrigation_history"), 1)
            central = self.central()
            central.handle_request(self.request())
            central.handle_event(self.event("END"))
            self.assertEqual(self.count("irrigation_history"), 2)


class OperatorTests(IsolatedTest):
    def test_stations_and_end_in_same_poll_are_preserved(self):
        raw = FakeConsumer()
        raw.add(dict(type="STATIONS", id="L", operator="FO-01"),
                dict(type="END", id="A", operator="FO-01", volume=1))
        inbox = fo.ResponseInbox(raw, "FO-01")
        self.assertEqual(next(inbox.wait("L", 1))["type"], "STATIONS")
        self.assertEqual(next(inbox.wait("A", 1))["type"], "END")

    def test_messages_from_other_operator_are_ignored(self):
        raw = FakeConsumer()
        raw.add(dict(type="END", id="A", operator="FO-02"),
                dict(type="ACK", id="A", operator="FO-01"))
        inbox = fo.ResponseInbox(raw, "FO-01")
        self.assertEqual(next(inbox.wait("A", 1))["type"], "ACK")

    def test_batch_continues_after_invalid_station_and_duration(self):
        path = Path(self.tmp.name) / "requests.txt"
        path.write_text("WS-99,2\nWS-01,abc\nWS-01,2\n", encoding="utf-8")
        raw = FakeConsumer()
        inbox = fo.ResponseInbox(raw, "FO-01")
        sent = []

        def publish(producer, topic, msg):
            sent.append(msg)
            raw.add(dict(type="END", id=msg["id"], operator="FO-01",
                         reason="COMPLETADO", elapsed=2, volume=.5))

        with patch.object(fo, "publish", side_effect=publish), patch.object(fo.time, "sleep") as sleep:
            fo.execute_file(None, inbox, "FO-01", path, [{"id": "WS-01"}])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(4), unittest.mock.call(4)])
        self.assertNotIn("pending", fo.load_fo_state("FO-01"))

    def test_unknown_result_pauses_batch_and_keeps_id(self):
        path = Path(self.tmp.name) / "requests.txt"
        path.write_text("WS-01,2\nWS-01,2\n", encoding="utf-8")
        with patch.object(fo, "publish") as sent, patch.object(fo, "wait_response", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                fo.execute_file(None, None, "FO-01", path, [{"id": "WS-01"}])
        self.assertEqual(sent.call_count, 1)
        self.assertEqual(fo.load_fo_state("FO-01")["pending"]["id"], sent.call_args.args[2]["id"])

    def test_recovery_resends_original_id_without_creating_another_order(self):
        request = self.request()
        fo.save_fo_state("FO-01", {"pending": request})
        raw = FakeConsumer()
        inbox = fo.ResponseInbox(raw, "FO-01")

        def publish(producer, topic, msg):
            self.assertEqual(msg["id"], request["id"])
            raw.add(dict(type="END", id=msg["id"], operator="FO-01",
                         reason="COMPLETADO", volume=1, elapsed=4))

        with patch.object(fo, "publish", side_effect=publish):
            fo.check_recovery(None, inbox, "FO-01", None, None)
        self.assertNotIn("pending", fo.load_fo_state("FO-01"))

    def test_completed_result_is_shown_once_without_five_minute_limit(self):
        result = dict(self.event("END"), updated_at=1, timestamp=1)
        with patch.object(fo, "show_result", wraps=fo.show_result) as show:
            fo.check_recovery(None, None, "FO-01", None, result)
            fo.check_recovery(None, None, "FO-01", None, result)
        self.assertEqual(show.call_count, 1)

    def test_invalid_manual_duration_returns_to_menu(self):
        raw = FakeConsumer()
        producer = types.SimpleNamespace(close=lambda: None)
        with patch.object(fo, "open_bus", return_value=(producer, raw)), \
             patch.object(fo, "get_stations", return_value=([dict(id="WS-01",status="AVAILABLE")],None,None)), \
             patch("builtins.input", side_effect=["2","WS-01","abc","4"]), \
             patch.object(sys, "argv", ["WM_FO","--kafka-broker","fake","--id-operario","FO-01"]):
            fo.main()
        self.assertTrue(raw.closed)


class EngineTests(IsolatedTest):
    def valve(self):
        link = threading.Event()
        link.set()
        return engine.Valve("WS-01", queue.Queue(), threading.Event(), link)

    def test_final_is_persisted_before_network_delivery(self):
        valve = self.valve()
        run = self.request()
        valve.current = run
        # Cierre inmediato controlado sin esperar un riego real de 30 segundos.
        valve.stop.set()
        valve.water(run)
        status, event = EngineJournal("WS-01").get("A")
        self.assertEqual(status, "FINAL_PENDING")
        self.assertEqual(event["type"], "END")

    def test_failed_send_keeps_result_and_success_marks_delivered(self):
        valve = self.valve()
        event = self.event("END")
        valve.journal.save(event, "FINAL_PENDING")
        valve.outbox.put(event)
        stopped = threading.Event()
        stopped.set()
        attempts = []

        def publish(*args):
            attempts.append(1)
            if len(attempts) == 1:
                raise TimeoutError("ACK perdido")

        with patch.object(engine, "publish", side_effect=publish), patch.object(engine.time, "sleep"):
            engine.sender(None, valve.outbox, stopped, valve)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(valve.journal.get("A")[0], "DELIVERED")

    def test_crashed_run_becomes_pending_end(self):
        journal = EngineJournal("WS-01")
        journal.save(dict(self.request(), volume=1, elapsed=4), "IN_PROGRESS")
        pending = EngineJournal("WS-01").recover()
        self.assertEqual(pending[0]["reason"], "INTERRUMPIDO_POR_CAIDA_ENGINE")
        self.assertEqual(pending[0]["volume"], 1)
        self.assertEqual(EngineJournal("WS-01").get("A")[0], "FINAL_PENDING")

    def test_duplicate_start_of_completed_run_never_opens_valve(self):
        valve = self.valve()
        valve.journal.save(self.event("END"), "FINAL_PENDING")
        valve.journal.delivered("A")
        with patch.object(threading, "Thread") as worker:
            valve.start(dict(self.request(), expires=time.time()+12))
        worker.assert_not_called()
        self.assertEqual(valve.outbox.get()["type"], "END")

    def test_rejected_start_cannot_run_later_after_repair(self):
        valve = self.valve()
        valve.fault.set()
        request = dict(self.request(), expires=time.time()+12)
        valve.start(request)
        valve.fault.clear()
        with patch.object(threading, "Thread") as worker:
            valve.start(request)
        worker.assert_not_called()
        self.assertEqual(valve.journal.get("A")[0], "FINAL_PENDING")

    def test_ack_for_old_run_does_not_erase_new_checkpoint(self):
        journal = EngineJournal("WS-01")
        journal.save(self.event("END","A"), "FINAL_PENDING")
        journal.save(dict(self.request("B"), volume=.5, elapsed=2), "IN_PROGRESS")
        journal.delivered("A")
        self.assertEqual(journal.get("B")[0], "IN_PROGRESS")

    def test_engine_checkpoint_survives_abrupt_process_exit(self):
        code = (
            "from wm_state import EngineJournal; import os; "
            "EngineJournal('WS-01').save({'id':'A','operator':'FO-01','volume':1,'elapsed':4},'IN_PROGRESS'); "
            "os._exit(17)"
        )
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True)
        self.assertEqual(result.returncode, 17, result.stderr.decode(errors="replace"))
        self.assertEqual(EngineJournal("WS-01").recover()[0]["volume"], 1)


class KafkaAdapterTests(unittest.TestCase):
    def test_central_seeks_saved_offset_and_new_partition_beginning(self):
        consumer = unittest.mock.Mock()
        partition = wm_common.TopicPartition("ws_events", 0)
        consumer.partitions_for_topic.return_value = {0}
        consumer.beginning_offsets.return_value = {partition: 2}
        with patch.object(wm_common, "KafkaAdminClient"), patch.object(wm_common, "KafkaProducer"), \
             patch.object(wm_common, "KafkaConsumer", return_value=consumer):
            wm_common.open_bus("fake", ["ws_events"], {("ws_events", 0): 8})
        consumer.seek.assert_called_once_with(partition, 8)
        consumer.end_offsets.assert_not_called()

    def test_first_central_start_reads_retained_log(self):
        consumer = unittest.mock.Mock()
        partition = wm_common.TopicPartition("ws_events", 0)
        consumer.partitions_for_topic.return_value = {0}
        consumer.beginning_offsets.return_value = {partition: 2}
        with patch.object(wm_common, "KafkaAdminClient"), patch.object(wm_common, "KafkaProducer"), \
             patch.object(wm_common, "KafkaConsumer", return_value=consumer):
            wm_common.open_bus("fake", ["ws_events"], {})
        consumer.seek.assert_called_once_with(partition, 2)

    def test_fo_starts_at_end_and_uses_central_for_recovery(self):
        consumer = unittest.mock.Mock()
        partition = wm_common.TopicPartition("fo_responses", 0)
        consumer.partitions_for_topic.return_value = {0}
        consumer.end_offsets.return_value = {partition: 20}
        with patch.object(wm_common, "KafkaAdminClient"), patch.object(wm_common, "KafkaProducer"), \
             patch.object(wm_common, "KafkaConsumer", return_value=consumer):
            wm_common.open_bus("fake", ["fo_responses"])
        consumer.seek.assert_called_once_with(partition, 20)


if __name__ == "__main__":
    unittest.main()
