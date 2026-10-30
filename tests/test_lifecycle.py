"""Cierre del lanzador y lote inicial: disco real, sin depender de un broker."""
import contextlib
import io
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import WM_FO.WM_FO as fo
import WM_WS.WM_WS_E.WM_WS_E as engine
from wm_state import EngineJournal


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP"))
        self.addCleanup(directory.cleanup)
        self.env = patch.dict(os.environ, {"WM_STATE_DIR": directory.name})
        self.env.start()
        self.addCleanup(self.env.stop)
        output = contextlib.redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)

    def test_fo_recovers_before_startup_file_and_returns_to_menu(self):
        order = []
        producer, consumer = Mock(), Mock()
        argv = ["WM_FO", "--kafka-broker", "fake", "--id-operario", "FO-01",
                "--file", "lote.txt"]
        stations = [{"id": "WS-01", "status": "AVAILABLE"}]
        with patch.object(sys, "argv", argv), \
             patch.object(fo, "open_bus", return_value=(producer, consumer)), \
             patch.object(fo, "get_stations", return_value=(stations, None, None)), \
             patch.object(fo, "check_recovery", side_effect=lambda *a: order.append("recover")), \
             patch.object(fo, "execute_file", side_effect=lambda *a: order.append("file")) as batch, \
             patch("builtins.input", side_effect=lambda *a: order.append("menu") or "4"):
            fo.main()
        self.assertEqual(order, ["recover", "file", "menu"])
        self.assertEqual(batch.call_args.args[3:], ("lote.txt", stations))
        consumer.close.assert_called_once()
        producer.close.assert_called_once()

    def test_fo_does_not_start_file_when_recovery_is_uncertain(self):
        argv = ["WM_FO", "--kafka-broker", "fake", "--id-operario", "FO-01",
                "--batch-file", "lote.txt"]
        with patch.object(sys, "argv", argv), \
             patch.object(fo, "open_bus", return_value=(Mock(), Mock())), \
             patch.object(fo, "get_stations", return_value=([], None, None)), \
             patch.object(fo, "check_recovery", side_effect=TimeoutError("sin confirmación")), \
             patch.object(fo, "execute_file") as batch, \
             patch("builtins.input", return_value="4"):
            fo.main()
        batch.assert_not_called()

    def test_fo_sigterm_closes_clients_and_restores_signal_handler(self):
        producer, consumer = Mock(), Mock()
        previous = signal.getsignal(signal.SIGTERM)
        argv = ["WM_FO", "--kafka-broker", "fake", "--id-operario", "FO-01"]
        with patch.object(sys, "argv", argv), \
             patch.object(fo, "open_bus", return_value=(producer, consumer)), \
             patch.object(fo, "get_stations", return_value=([], None, None)), \
             patch.object(fo, "check_recovery"), \
             patch("builtins.input", side_effect=lambda *a: signal.raise_signal(signal.SIGTERM)):
            fo.main()
        consumer.close.assert_called_once()
        producer.close.assert_called_once()
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_engine_sigterm_persists_final_and_sends_before_sender_exits(self):
        producer, consumer = Mock(), Mock()
        ready = threading.Event()
        sent = []
        previous = signal.getsignal(signal.SIGTERM)

        def monitor(args, station, link, fault, stopped, valve, initial):
            link.set()
            ready.set()
            stopped.wait()

        def incoming(raw):
            self.assertTrue(ready.wait(timeout=2))
            yield dict(type="START", id="run-1", ws="WS-01", operator="FO-01",
                       duration=30, expires=time.time() + 10)
            # Pasa por el mismo handler registrado para SIGTERM dentro de Docker.
            signal.raise_signal(signal.SIGTERM)

        argv = ["WM_WS_E", "--kafka-broker", "fake", "--monitor-port", "7000"]
        reader = Mock()
        reader.read.return_value = "IDENTITY#WS-01"
        with patch.object(sys, "argv", argv), \
             patch.object(engine, "open_bus", return_value=(producer, consumer)), \
             patch.object(engine.socket, "create_connection", return_value=Mock()), \
             patch.object(engine, "LineReader", return_value=reader), \
             patch.object(engine, "monitor_loop", side_effect=monitor), \
             patch.object(engine, "keyboard"), \
             patch.object(engine, "messages", side_effect=incoming), \
             patch.object(engine, "publish", side_effect=lambda p, t, m: sent.append(m)):
            engine.main()
        status, result = EngineJournal("WS-01").get("run-1")
        self.assertEqual(status, "DELIVERED")
        self.assertEqual(result["reason"], "APAGADO_ENGINE")
        self.assertEqual(sum(m["type"] == "END" for m in sent), 1)
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)
        consumer.close.assert_called_once()
        producer.close.assert_called_once_with(timeout=2)

    def test_engine_keeps_final_pending_if_sender_cannot_confirm(self):
        link = threading.Event()
        link.set()
        import queue
        valve = engine.Valve("WS-01", queue.Queue(), threading.Event(), link)
        valve.start(dict(id="run-2", operator="FO-01", duration=30, expires=time.time()+10))
        runtime_stop, sender_stop = threading.Event(), threading.Event()
        sender = Mock()
        sender.is_alive.return_value = True
        engine.shutdown(valve, runtime_stop, sender_stop, sender, Mock(), Mock())
        self.assertFalse(valve.worker.is_alive())
        self.assertTrue(runtime_stop.is_set())
        self.assertTrue(sender_stop.is_set())
        status, result = EngineJournal("WS-01").get("run-2")
        self.assertEqual(status, "FINAL_PENDING")
        self.assertEqual(result["reason"], "APAGADO_ENGINE")


if __name__ == "__main__":
    unittest.main()
