"""Contrato del BAT: leer el proyecto descargado, sin empaquetar la aplicación."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

from tools.build_bat import ROOT
from tests.test_recovery import IsolatedTest
from WM_Central.WM_Central import create_app


class CatalogTests(IsolatedTest):
    def test_registers_new_ids_without_restart_and_preserves_existing(self):
        central = self.central()
        client = create_app(central).test_client()
        response = client.post("/api/catalog", json={
            "operators": [{"id": "FO-12", "name": "Operario 12"}],
            "stations": [{"id": "WS-12", "location": "Parque 12"},
                         {"id": "WS-01", "location": "No sobrescribir"}],
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(central.stations["WS-01"]["location"], "Parque Central")
        self.assertEqual(central.status(central.stations["WS-12"]), "OFFLINE")
        self.assertEqual(client.post("/api/catalog", json={
            "stations": [{"id": "WS-12", "location": "Otro"}]}).status_code, 200)
        self.assertEqual(central.stations["WS-12"]["location"], "Parque 12")
        self.assertEqual(self.count("watering_stations"), 4)
        self.assertEqual(self.count("operators"), 4)

    def test_invalid_batch_does_not_partially_register(self):
        client = create_app(self.central()).test_client()
        response = client.post("/api/catalog", json={
            "operators": [{"id": "FO-15", "name": "Operario 15"}],
            "stations": [{"id": "WS-15", "location": "Con#separador"}],
        })
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.count("operators"), 3)

    def test_catalog_rejects_malformed_bodies(self):
        client = create_app(self.central()).test_client()
        for body in (None, [], {"stations": {}}, {"stations": [None]},
                     {"operators": [{"id": "FO-00", "name": "Mal"}]},
                     {"unexpected": []}):
            with self.subTest(body=body):
                self.assertEqual(client.post("/api/catalog", json=body).status_code, 400)


class PortableBatTests(unittest.TestCase):
    def test_bat_contains_only_the_current_launcher(self):
        text = (ROOT / "WaterManagement.bat").read_text(encoding="utf-8")
        launcher = text.split("\n#<WM_POWERSHELL>\n", 1)[1].rsplit("\n#<WM_END>", 1)[0]
        self.assertEqual(launcher.strip(), (ROOT / "tools/launcher.ps1").read_text(encoding="utf-8").strip())
        self.assertNotIn("#<WM_PAYLOAD>", text)
        self.assertNotIn("class Central:", text)
        self.assertNotIn("class Valve:", text)

    def make_project(self, folder):
        for name in ("Dockerfile", ".dockerignore", "requirements.txt", "wm_common.py", "wm_state.py",
                     "WM_Central/database.py", "WM_Central/WM_Central.py", "WM_Dashboard/index.html",
                     "WM_FO/WM_FO.py", "WM_WS/WM_WS_M/WM_WS_M.py", "WM_WS/WM_WS_E/WM_WS_E.py"):
            path = folder / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Fuente descargada\n", encoding="utf-8")

    def run_bat(self, bat, option="--check", source=""):
        return subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", "& $env:WM_PORTABLE_BAT " + option + " $env:WM_TEST_SOURCE"],
            env=dict(os.environ, WM_PORTABLE_BAT=str(bat), WM_TEST_SOURCE=str(source), WM_LAUNCHER_TEST=""),
            cwd=bat.parent, capture_output=True, timeout=30,
        )

    @unittest.skipUnless(shutil.which("powershell.exe"), "Requiere Windows PowerShell")
    def test_bat_finds_adjacent_project_or_sd_subfolder_without_creating_files(self):
        with tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP")) as directory:
            folder = Path(directory)
            bat = folder / "Práctica con espacios.bat"
            shutil.copyfile(ROOT / "WaterManagement.bat", bat)
            self.make_project(folder / "sd")
            before = set(folder.rglob("*"))
            result = self.run_bat(bat)
            self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
            self.assertIn(b"Proyecto correcto", result.stdout)
            self.assertEqual(set(folder.rglob("*")), before)
            bat_inside = folder / "sd" / bat.name
            shutil.copyfile(bat, bat_inside)
            result = self.run_bat(bat_inside)
            self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))

    @unittest.skipUnless(shutil.which("powershell.exe"), "Requiere Windows PowerShell")
    def test_bat_uses_explicit_external_project_path_with_spaces_and_accents(self):
        with tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP")) as directory:
            folder = Path(directory)
            bat = folder / "Lanzador.bat"
            shutil.copyfile(ROOT / "WaterManagement.bat", bat)
            source = folder / "Código descargado" / "proyecto"
            self.make_project(source)
            result = self.run_bat(bat, source=source)
            self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
            self.assertIn(b"Proyecto correcto", result.stdout)
            original_bat = bat.read_bytes()
            before_hash = re.search(rb"[0-9a-f]{64}", result.stdout).group()
            (source / "WM_FO/WM_FO.py").write_text("# Código modificado después de descargar\n", encoding="utf-8")
            updated = self.run_bat(bat, source=source)
            self.assertEqual(updated.returncode, 0, updated.stderr.decode(errors="replace"))
            self.assertNotEqual(before_hash, re.search(rb"[0-9a-f]{64}", updated.stdout).group())
            self.assertEqual(bat.read_bytes(), original_bat)

    @unittest.skipUnless(shutil.which("powershell.exe"), "Requiere Windows PowerShell")
    def test_missing_sources_fail_clearly_without_using_a_stored_copy(self):
        with tempfile.TemporaryDirectory(dir=os.getenv("WM_TEST_TMP")) as directory:
            bat = Path(directory) / "Lanzador.bat"
            shutil.copyfile(ROOT / "WaterManagement.bat", bat)
            result = self.run_bat(bat)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(Path(directory).iterdir()), [bat])
            self.assertEqual(self.run_bat(bat, "--help").returncode, 0)


if __name__ == "__main__":
    unittest.main()
