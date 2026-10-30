"""Una consola visible por contenedor; nunca se matan procesos por su nombre."""
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import threading


def launcher_command():
    if getattr(sys, "frozen", False):
        return [sys.executable]
    return [sys.executable, str(Path(__file__).resolve().parents[1] / "wm.py")]


def process_identity(pid):
    """La fecha de creacion impide confundir un PID reciclado con nuestra ventana."""
    if os.name != "nt":
        return None
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return None
    try:
        code = wintypes.DWORD()
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value != 259:
            return None
        created, exited, user, system = (wintypes.FILETIME() for _ in range(4))
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
        if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                      ctypes.byref(system), ctypes.byref(user)):
            return None
        return (created.dwHighDateTime << 32) | created.dwLowDateTime
    finally:
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle(handle)


def open_console(container, directory, enabled=True):
    if not enabled:
        return
    if os.name != "nt":
        print(f"[CONSOLA] En Linux usa: docker attach {container['Id'][:12]}")
        return
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    lease = directory / (container["Id"] + ".json")
    try:
        previous = json.loads(lease.read_text(encoding="utf-8"))
        if previous["created"] is not None and process_identity(previous["pid"]) == previous["created"]:
            return  # El componente y su ventana ya existen: no abrir duplicados.
    except (OSError, ValueError, KeyError):
        pass
    # Sin shell: ni los espacios en rutas ni los IDs se interpretan como comandos.
    process = subprocess.Popen(
        launcher_command() + ["_console", "--container", container["Id"]],
        creationflags=subprocess.CREATE_NEW_CONSOLE,
        # Cada ventana extrae su propio paquete. Asi sigue viva aunque el usuario
        # salga del menu y el .exe principal borre su carpeta temporal de PyInstaller.
        env=dict(os.environ, PYINSTALLER_RESET_ENVIRONMENT="1"),
    )
    lease.write_text(json.dumps({"pid": process.pid, "created": process_identity(process.pid)}),
                     encoding="utf-8")


def console_worker(container_id):
    """Attach conserva input/teclas. Al parar el contenedor la ventana se cierra sola."""
    result = subprocess.run(["docker", "inspect", "--type", "container", container_id],
                            capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode:
        print("El contenedor ya no existe.")
        return 1
    container = json.loads(result.stdout)[0]
    labels = container["Config"].get("Labels") or {}
    if not labels.get("org.watermanagement.launcher"):
        print("Este contenedor no pertenece al lanzador.")
        return 1
    if os.name == "nt":
        ctypes.windll.kernel32.SetConsoleTitleW(container["Name"].lstrip("/"))
    print("WATER MANAGEMENT | " + container["Name"].lstrip("/"), flush=True)
    print("Cerrar esta ventana simula la caida del componente. CLEAN hace un cierre ordenado.",
          flush=True)
    finished = threading.Event()
    handler = None
    if os.name == "nt":
        # Windows da unos segundos al cerrar una consola. Se mata SOLO este ID
        # inmutable de Docker, nunca otro contenedor creado despues con igual nombre.
        # Windows BOOL tiene 32 bits (no el bool de un byte de C/Python).
        handler_type = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_uint)

        @handler_type
        def handler(event):
            if event in (2, 5, 6) and not finished.is_set():
                try:
                    subprocess.run(["docker", "kill", container_id], capture_output=True,
                                   timeout=4, creationflags=subprocess.CREATE_NO_WINDOW)
                except (OSError, subprocess.TimeoutExpired):
                    pass
                return True
            return False

        ctypes.windll.kernel32.SetConsoleCtrlHandler(handler, True)
    kind = labels.get("org.watermanagement.kind")
    command = (["docker", "logs", "--follow", "--tail", "100", container_id]
               if kind == "monitor" else ["docker", "attach", container_id])
    try:
        if kind != "monitor":
            # run -dit puede imprimir el menu antes de que Windows abra la ventana.
            # attach solo muestra datos futuros: recuperamos primero el menu ya escrito.
            subprocess.call(["docker", "logs", "--tail", "40", container_id])
        # Docker attach recibe el teclado real de ESTA consola, no el del menu.
        return subprocess.call(command)
    except KeyboardInterrupt:
        return 0
    finally:
        finished.set()
        if handler is not None:
            ctypes.windll.kernel32.SetConsoleCtrlHandler(handler, False)
