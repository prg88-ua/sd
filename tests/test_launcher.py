"""Escenarios del lanzador con Docker y API simulados; no abre ventanas ni servicios."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wm
import wm_launcher.windows as windows_module
from wm_launcher.runtime import ENTITY, KIND, OWNER, PROFILE, LauncherError, Runner, quantity


class DockerStore:
    """Estado externo compartido: reiniciar Runner no pierde los contenedores."""
    def __init__(self):
        self.calls = []
        self.api_calls = []
        self.containers = {}
        self.networks = {}
        self.volumes = set()
        self.image_id = "sha256:test-image-1"
        self.next_id = 1
        self.api_failure = None
        self.operators = set()
        self.stations = set()


class FakeRunner(Runner):
    def __init__(self, config, home, store):
        super().__init__(config, home, windows=False)
        self.store = store

    def source_digest(self):
        return "test-source-digest"

    def api(self, path, body=None):
        self.store.api_calls.append((path, body))
        if self.store.api_failure:
            raise LauncherError(self.store.api_failure)
        if path == "/health":
            return {"kafka": True}
        if path == "/api/admin/catalog":
            self.store.operators.update(row["id"] for row in body["operators"])
            self.store.stations.update(row["id"] for row in body["stations"])
            return {"created": {"operators": len(body["operators"]), "stations": len(body["stations"])}}
        if path == "/api/stations":
            return [{"id": identity, "status": "AVAILABLE"} for identity in self.store.stations]
        raise AssertionError(f"API inesperada: {path}")

    def docker(self, *arguments, check=True, **kwargs):
        args = tuple(map(str, arguments))
        self.store.calls.append(args)
        store = self.store

        def result(stdout="", stderr="", code=0):
            if code and check:
                raise LauncherError(stderr)
            return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=code)

        def container(value):
            return next((row for row in store.containers.values()
                         if row["Id"] == value or row["Name"].lstrip("/") == value), None)

        if args[0] == "info":
            return result("linux\n")
        if args[:2] == ("image", "inspect"):
            return result(json.dumps([{"Id": store.image_id,
                                      "Config": {"Labels": {OWNER + ".source": "test-source-digest"}}}]))
        if args[0] == "ps":
            filters = [args[index + 1] for index, value in enumerate(args) if value == "--filter"]
            matches = list(store.containers.values())
            for item in filters:
                self.assert_filter(item)
                key, value = item[len("label="):].split("=", 1)
                matches = [row for row in matches if row["Config"]["Labels"].get(key) == value]
            return result("\n".join(row["Id"] for row in matches))
        if args[0] == "inspect":
            selected = args[3:] if args[1:3] == ("--type", "container") else args[1:]
            rows = [container(value) for value in selected]
            if not all(rows):
                return result(stderr="Error: No such container", code=1)
            return result(json.dumps(rows))
        if args[:2] == ("network", "inspect"):
            if args[2] not in store.networks:
                return result(stderr="No such network", code=1)
            info = dict(store.networks[args[2]])
            info["Containers"] = {row["Id"]: {} for row in store.containers.values()
                                  if row["Network"] == args[2]}
            return result(json.dumps([info]))
        if args[:2] == ("network", "create"):
            label = args[args.index("--label") + 1]
            key, value = label.split("=", 1)
            store.networks[args[-1]] = {"Labels": {key: value}}
            return result(args[-1])
        if args[:2] == ("network", "rm"):
            del store.networks[args[2]]
            return result()
        if args[0] == "run":
            labels = dict(args[index + 1].split("=", 1) for index, value in enumerate(args)
                          if value == "--label")
            volumes = [args[index + 1] for index, value in enumerate(args) if value == "--volume"]
            mounts = [args[index + 1] for index, value in enumerate(args) if value == "--mount"]
            image_index = args.index(store.image_id)
            name = args[args.index("--name") + 1]
            if container(name):
                return result(stderr="name already exists", code=1)
            identity = f"container-{store.next_id:04d}"
            store.next_id += 1
            store.volumes.update(volume.rsplit(":", 1)[0] for volume in volumes)
            store.containers[identity] = {
                "Id": identity, "Name": "/" + name,
                "Config": {"Labels": labels, "Cmd": list(args[image_index + 1:])},
                "State": {"Running": True, "Status": "running"},
                "Network": args[args.index("--network") + 1],
                "Volumes": volumes, "BindMounts": mounts,
            }
            return result(identity)
        if args[0] in ("start", "stop", "kill"):
            row = container(args[-1])
            if not row:
                return result(stderr="No such container", code=1)
            running = args[0] == "start"
            row["State"] = {"Running": running, "Status": "running" if running else "exited"}
            return result()
        if args[0] == "rm":
            row = container(args[-1])
            if row["State"]["Running"]:
                raise AssertionError("No se puede borrar un contenedor vivo en estas pruebas")
            del store.containers[row["Id"]]
            return result()
        if args[0] == "compose":
            return result()
        raise AssertionError(f"Docker inesperado: {args}")

    @staticmethod
    def assert_filter(value):
        if not value.startswith("label="):
            raise AssertionError("El inventario debe filtrar por etiquetas propias")


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP"))
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.store = DockerStore()
        self.runner = self.make_runner()
        output = contextlib.redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)
        console_patch = patch("wm_launcher.runtime.open_console")
        self.consoles = console_patch.start()
        self.addCleanup(console_patch.stop)

    def make_runner(self, role="fo", namespace="test-wm"):
        config = dict(role=role, aws_host="192.0.2.10", central_port=6000, api_port=5000,
                      kafka_port=9092, namespace=namespace, image="water-management:local",
                      cloud_project="existing-cloud-project")
        return FakeRunner(config, self.home, self.store)

    def running(self, runner=None):
        return (runner or self.runner).active_ids()

    def mutations(self):
        return [args for args in self.store.calls if args[0] in ("run", "start", "stop", "kill", "rm")
                or args[:2] in (("network", "create"), ("network", "rm"))]

    def test_add_is_incremental_and_same_id_is_reused_after_launcher_restart(self):
        wm.execute(self.runner, "add", SimpleNamespace(count=2, file=None))
        self.assertEqual(self.running(), {"FO-01", "FO-02"})
        restarted = self.make_runner()
        wm.execute(restarted, "add", SimpleNamespace(count=2, file=None))
        self.assertEqual(self.running(), {"FO-01", "FO-02", "FO-03", "FO-04"})
        before = len(self.store.containers)
        restarted.start_ids(["FO-02"])
        self.assertEqual(len(self.store.containers), before)
        self.assertEqual(sum(args[0] == "run" for args in self.store.calls), 4)

    def test_set_exact_count_stops_extras_but_preserves_their_data(self):
        self.runner.set_count(4)
        volumes = set(self.store.volumes)
        self.runner.set_count(2)
        self.assertEqual(self.running(), {"FO-01", "FO-02"})
        self.assertEqual(len(self.store.containers), 4)
        self.assertEqual(self.store.volumes, volumes)
        self.assertFalse(any(args[0] == "rm" for args in self.store.calls))

    def test_set_does_not_reconfigure_or_rerun_an_active_batch_operator(self):
        batch = self.home / "requests.txt"
        batch.write_text("WS-01,5\n", encoding="utf-8")
        self.runner.start_ids(["FO-01"], batch_file=batch)
        original = next(iter(self.store.containers))
        command = list(self.store.containers[original]["Config"]["Cmd"])
        self.store.calls.clear()
        self.runner.set_count(2)
        self.assertEqual(self.running(), {"FO-01", "FO-02"})
        self.assertEqual(self.store.containers[original]["Config"]["Cmd"], command)
        runs = [args for args in self.store.calls if args[0] == "run"]
        self.assertEqual(len(runs), 1)
        self.assertIn(self.runner.container_name("fo", "FO-02"), runs[0])
        self.assertFalse(any(args[0] in ("start", "stop", "rm") for args in self.store.calls))

    def test_set_recovers_only_missing_part_of_an_incomplete_station(self):
        ws = self.make_runner("ws")
        ws.start_ids(["WS-01"], part="monitor")
        monitor_id = next(iter(self.store.containers))
        self.store.calls.clear()
        ws.set_count(1)
        self.assertEqual(len(self.store.containers), 2)
        self.assertTrue(self.store.containers[monitor_id]["State"]["Running"])
        runs = [args for args in self.store.calls if args[0] == "run"]
        self.assertEqual(len(runs), 1)
        self.assertIn(ws.container_name("engine", "WS-01"), runs[0])

    def test_set_zero_closes_all_even_if_aws_is_unavailable(self):
        self.runner.set_count(2)
        self.store.api_failure = "AWS no disponible"
        self.runner.set_count(0)
        self.assertEqual(self.running(), set())
        self.assertEqual(len(self.store.volumes), 2)

    def test_set_zero_from_cli_is_noninteractive(self):
        self.runner.set_count(2)
        with patch("builtins.input", side_effect=AssertionError("No debe pedir datos para cerrar")):
            wm.execute(self.runner, "set", SimpleNamespace(count=0, file=None))
        self.assertEqual(self.running(), set())

    def test_clean_only_removes_own_profile_and_keeps_other_containers_and_volumes(self):
        ws = self.make_runner("ws")
        ws.start_ids(["WS-01"])
        self.runner.start_ids(["FO-01"])
        foreign = self.make_runner("fo", "another-project")
        foreign.start_ids(["FO-01"])
        protected = {row["Id"] for row in self.store.containers.values()
                     if row["Config"]["Labels"].get(PROFILE) != "ws"}
        volumes = set(self.store.volumes)
        self.store.calls.clear()
        ws.clean()
        self.assertEqual(set(self.store.containers), protected)
        self.assertEqual(self.store.volumes, volumes)
        self.assertIn(ws.network, self.store.networks)  # FO usa la misma red; no debe borrarse.
        self.assertFalse(any("prune" in args or "--volumes" in args or "-v" in args
                             for args in self.store.calls))
        stopped = [args[-1] for args in self.store.calls if args[0] == "stop"]
        self.assertEqual(len(stopped), 2)

    def test_clean_cloud_uses_its_compose_project_without_deleting_volumes(self):
        cloud = self.make_runner("cloud")
        cloud.clean()
        calls = [args for args in self.store.calls if args[0] == "compose"]
        self.assertEqual(len(calls), 1)
        self.assertIn("down", calls[0])
        self.assertIn(cloud.config["cloud_project"], calls[0])
        self.assertNotIn("--volumes", calls[0])
        self.assertNotIn("-v", calls[0])

    def test_changed_image_does_not_replace_a_running_container(self):
        self.runner.start_ids(["FO-01"])
        original = next(iter(self.store.containers))
        self.store.image_id = "sha256:test-image-2"
        self.runner._image_id = None
        self.store.calls.clear()
        with self.assertRaises(LauncherError):
            self.runner.start_ids(["FO-01"])
        self.assertTrue(self.store.containers[original]["State"]["Running"])
        self.assertFalse(any(args[0] in ("run", "stop", "rm", "kill") for args in self.store.calls))

    def test_stopped_old_image_is_replaced_using_the_same_persistent_volume(self):
        self.runner.start_ids(["FO-01"])
        original = next(iter(self.store.containers))
        volumes = set(self.store.volumes)
        self.runner.stop_id("FO-01")
        self.store.image_id = "sha256:test-image-2"
        self.runner._image_id = None
        self.runner.start_ids(["FO-01"])
        self.assertNotIn(original, self.store.containers)
        self.assertEqual(self.running(), {"FO-01"})
        self.assertEqual(self.store.volumes, volumes)

    def test_foreign_container_with_same_name_is_never_modified(self):
        self.runner.start_ids(["FO-01"])
        row = next(iter(self.store.containers.values()))
        row["Config"]["Labels"][OWNER] = "foreign-owner"
        self.store.calls.clear()
        with self.assertRaises(LauncherError):
            self.runner.start_ids(["FO-01"])
        self.assertEqual(self.mutations(), [])
        self.assertTrue(row["State"]["Running"])

    def test_station_is_a_monitor_engine_pair_using_container_dns(self):
        ws = self.make_runner("ws")
        ws.start_ids(["WS-01", "WS-02"])
        self.assertEqual(len(self.store.containers), 4)
        for row in self.store.containers.values():
            if row["Config"]["Labels"][KIND] == "engine":
                command = row["Config"]["Cmd"]
                entity = row["Config"]["Labels"][ENTITY]
                self.assertEqual(command[command.index("--monitor-host") + 1],
                                 ws.container_name("monitor", entity))
        for args in self.store.calls:
            if args[0] == "run":
                self.assertNotIn("host", args)
                self.assertNotIn("--publish", args)

    def test_monitor_can_start_alone_without_waiting_for_a_nonexistent_engine(self):
        ws = self.make_runner("ws")
        with patch.object(ws, "wait_stations", side_effect=AssertionError("Monitor solo no está AVAILABLE")):
            ws.start_ids(["WS-01"], part="monitor")
        self.assertEqual(len(self.store.containers), 1)
        row = next(iter(self.store.containers.values()))
        self.assertEqual(row["Config"]["Labels"][KIND], "monitor")

    def test_engine_stops_before_monitor_and_crash_only_kills_requested_part(self):
        ws = self.make_runner("ws")
        ws.start_ids(["WS-01"])
        by_kind = {row["Config"]["Labels"][KIND]: row["Id"]
                   for row in self.store.containers.values()}
        self.store.calls.clear()
        ws.stop_id("WS-01")
        self.assertEqual([args[-1] for args in self.store.calls if args[0] == "stop"],
                         [by_kind["engine"], by_kind["monitor"]])
        ws.start_ids(["WS-01"])
        self.store.calls.clear()
        ws.stop_id("WS-01", part="engine", crash=True)
        self.assertEqual([args for args in self.store.calls if args[0] == "kill"],
                         [("kill", by_kind["engine"])])
        self.assertTrue(self.store.containers[by_kind["monitor"]]["State"]["Running"])

    def test_batch_file_with_spaces_is_mounted_readonly_and_passed_as_one_argument(self):
        directory = self.home / "lotes con espacios $x"
        directory.mkdir()
        batch = directory / "turno;1.txt"
        batch.write_text("WS-01,5\n", encoding="utf-8")
        self.runner.start_ids(["FO-01"], batch_file=batch)
        row = next(iter(self.store.containers.values()))
        self.assertEqual(len(row["BindMounts"]), 1)
        self.assertIn("readonly", row["BindMounts"][0])
        # Se monta SOLO el archivo seleccionado, sin exponer sus archivos hermanos.
        self.assertIn("source=" + str(batch.resolve()) + ",", row["BindMounts"][0])
        self.assertIn("--file", row["Config"]["Cmd"])
        command = row["Config"]["Cmd"]
        self.assertTrue(command[command.index("--file") + 1].startswith("/requests/"))

    def test_missing_or_comma_batch_path_is_rejected_before_any_external_action(self):
        comma = self.home / "folder,comma"
        comma.mkdir()
        batch = comma / "requests.txt"
        batch.write_text("WS-01,5\n", encoding="utf-8")
        for value in (self.home / "missing.txt", batch):
            with self.subTest(path=value), self.assertRaises(LauncherError):
                self.runner.start_ids(["FO-01"], batch_file=value)
        self.assertEqual(self.store.calls, [])
        self.assertEqual(self.store.api_calls, [])

    def test_opening_from_cli_needs_no_interactive_credentials(self):
        with patch("builtins.input", side_effect=AssertionError("No debe pedir datos para abrir")):
            wm.execute(self.runner, "set", SimpleNamespace(count=2, file=None))
        self.assertEqual(self.running(), {"FO-01", "FO-02"})

    def test_aws_error_during_set_preserves_all_live_instances(self):
        self.runner.start_ids(["FO-01", "FO-03"])
        self.store.api_failure = "AWS no disponible"
        self.store.calls.clear()
        with self.assertRaises(LauncherError):
            self.runner.set_count(2)
        self.assertEqual(self.running(), {"FO-01", "FO-03"})
        self.assertEqual(self.mutations(), [])

    def test_invalid_quantities_and_ids_have_no_external_effects(self):
        for value in (-1, 1001, True, 1.5, "3", None):
            with self.subTest(quantity=value):
                with self.assertRaises(LauncherError):
                    quantity(value)
                with self.assertRaises(LauncherError):
                    self.runner.set_count(value)
                with self.assertRaises(LauncherError):
                    self.runner.next_ids(value)
        for identity in ("", "FO 1", "FO/01", "$(echo hi)", "a" * 81):
            with self.subTest(identity=identity), self.assertRaises(LauncherError):
                self.runner.start_ids([identity])
        self.assertEqual(self.store.calls, [])
        self.assertEqual(self.store.api_calls, [])

    def test_real_api_reports_old_server_without_prompting_credentials(self):
        runner = Runner(self.runner.config, self.home, windows=False)
        opener = SimpleNamespace(open=lambda request, timeout: (_ for _ in ()).throw(
            urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)))
        with patch("wm_launcher.runtime.urllib.request.build_opener", return_value=opener):
            with self.assertRaises(LauncherError) as caught:
                runner.register(operators=["FO-01"])
        self.assertIn("Actualiza", str(caught.exception))

    def test_real_catalog_request_sends_no_authorization_header(self):
        runner = Runner(self.runner.config, self.home, windows=False)
        captured = []

        def respond(request, timeout):
            captured.append(request)
            return io.BytesIO(b'{"created":{"operators":1,"stations":0}}')

        with patch("wm_launcher.runtime.urllib.request.build_opener",
                   return_value=SimpleNamespace(open=respond)):
            self.assertEqual(runner.register(operators=["FO-01"])["created"]["operators"], 1)
        self.assertFalse(captured[0].has_header("Authorization"))
        self.assertEqual(json.loads(captured[0].data)["operators"][0]["id"], "FO-01")

    def test_live_console_lease_is_reused_but_recycled_pid_opens_a_new_window(self):
        container = {"Id": "immutable-docker-id"}
        directory = self.home / "consoles"
        directory.mkdir()
        lease = directory / (container["Id"] + ".json")
        lease.write_text(json.dumps({"pid": 100, "created": 200}), encoding="utf-8")
        fake_os = SimpleNamespace(name="nt", environ=dict(os.environ))
        with patch.object(windows_module, "os", fake_os), \
                patch.object(windows_module.subprocess, "CREATE_NEW_CONSOLE", 16, create=True), \
                patch.object(windows_module, "launcher_command", return_value=["launcher.exe"]), \
                patch.object(windows_module, "process_identity", return_value=200), \
                patch.object(windows_module.subprocess, "Popen") as spawn:
            windows_module.open_console(container, directory)
            spawn.assert_not_called()
        # Mismo PID, distinta fecha: la ventana anterior terminó y el PID se recicló.
        with patch.object(windows_module, "os", fake_os), \
                patch.object(windows_module.subprocess, "CREATE_NEW_CONSOLE", 16, create=True), \
                patch.object(windows_module, "launcher_command", return_value=["launcher.exe"]), \
                patch.object(windows_module, "process_identity", return_value=201), \
                patch.object(windows_module.subprocess, "Popen", return_value=SimpleNamespace(pid=101)) as spawn:
            windows_module.open_console(container, directory)
            spawn.assert_called_once()
            self.assertEqual(spawn.call_args.args[0],
                             ["launcher.exe", "_console", "--container", container["Id"]])
        self.assertEqual(json.loads(lease.read_text(encoding="utf-8")), {"pid": 101, "created": 201})


if __name__ == "__main__":
    unittest.main()
