"""Empaquetado reproducible. No mete .git, claves, bases de datos ni historial."""
import argparse
from pathlib import Path
import subprocess
import sys
import ssl
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wm_launcher.runtime import SOURCE_DIRS, SOURCE_FILES


def make_sources(archive):
    paths = [ROOT / x for x in SOURCE_FILES]
    paths += [p for directory in SOURCE_DIRS for p in (ROOT / directory).rglob("*")
              if p.is_file() and p.suffix in (".py", ".html", ".js", ".css")]
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
        for path in sorted(paths):
            zipped.write(path, path.relative_to(ROOT).as_posix())


def main():
    parser = argparse.ArgumentParser(description="Crear WaterManagement.exe para Windows")
    parser.add_argument("--onedir", action="store_true", help="Version de diagnostico antes de onefile")
    args = parser.parse_args()
    if sys.platform != "win32":
        parser.error("El ejecutable Windows se construye desde Windows.")
    work = ROOT / ".wm_state" / "packaging"
    work.mkdir(parents=True, exist_ok=True)
    archive = work / "app_src.zip"
    make_sources(archive)
    command = [sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "--console",
               "--onedir" if args.onedir else "--onefile", "--name", "WaterManagement",
               "--distpath", str(ROOT / "dist"), "--workpath", str(work / "build"),
               "--specpath", str(work), "--paths", str(ROOT),
               "--add-data", str(archive) + ";."]
    # Este runtime puede acompañar ssl.py de una lista pública de certificados.
    # Si existe, también debe viajar junto al módulo SSL dentro del ejecutable.
    # No se cambia la verificación TLS ni se incorporan claves privadas.
    ssl_resource = Path(ssl.__file__).with_name("codex-rejected-cas.pem")
    if ssl_resource.is_file():
        command += ["--add-data", str(ssl_resource) + ";."]
    command.append(str(ROOT / "wm.py"))
    subprocess.run(command, check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
