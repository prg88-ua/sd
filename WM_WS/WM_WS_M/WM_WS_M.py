"""
WM_WS_M.py — Monitor de Estación de Riego
Actúa como puente permanente entre Central y Engine.

Flujo de hilos:
  Main        : conecta a Central, envía REGISTRO, se queda leyendo respuestas
  engine_srv  : abre servidor TCP en --monitor-port esperando al Engine
  ping_loop   : cada 1s envía PING#<n> al Engine y recoge PONG#<n>#OK|KO
  health_loop : cada 1s envía HEALTH#<ws_id>#OK|KO a Central
"""

import argparse
import socket
import threading
import time

FORMAT = "utf-8"
MAX_MESSAGE_BYTES = 4096
HEALTH_INTERVAL = 1.0   # segundos entre latidos a Central
PING_INTERVAL   = 1.0   # segundos entre pings al Engine
PING_TIMEOUT    = 2.0   # espera máxima para recibir el PONG

# ─────────────────────────────────────────────
# Utilidades de envío / recepción
# ─────────────────────────────────────────────

def send_msg(sock, text):
    """Envía un mensaje terminado en \\n."""
    sock.sendall((text + "\n").encode(FORMAT))


def recv_line(sock, timeout=None):
    """
    Lee del socket hasta el primer \\n y devuelve la línea limpia.
    Levanta socket.timeout si no llega nada en `timeout` segundos.
    Levanta ConnectionError si el socket se cierra.
    """
    old_timeout = sock.gettimeout()
    if timeout is not None:
        sock.settimeout(timeout)
    try:
        buf = b""
        while True:
            chunk = sock.recv(1024)
            if not chunk:
                raise ConnectionError("Socket cerrado por el otro extremo.")
            buf += chunk
            if b"\n" in buf:
                line, _ = buf.split(b"\n", 1)
                if len(line) > MAX_MESSAGE_BYTES:
                    raise ValueError("Línea demasiado larga.")
                return line.decode(FORMAT)
    finally:
        sock.settimeout(old_timeout)


# ─────────────────────────────────────────────
# Estado compartido entre hilos
# ─────────────────────────────────────────────

class MonitorState:
    def __init__(self):
        self.lock = threading.Lock()

        # Referencia al socket del Engine (None si no está conectado)
        self._engine_sock = None

        # True  → Engine respondió OK en el último PONG
        # False → Engine respondió KO, no respondió o se cayó
        self._engine_ok = False

        # Señal de parada global para todos los hilos
        self._stop = threading.Event()

    # ── Engine socket ────────────────────────
    @property
    def engine_sock(self):
        with self.lock:
            return self._engine_sock

    @engine_sock.setter
    def engine_sock(self, s):
        with self.lock:
            self._engine_sock = s

    # ── engine_ok ───────────────────────────
    @property
    def engine_ok(self):
        with self.lock:
            return self._engine_ok

    @engine_ok.setter
    def engine_ok(self, v):
        with self.lock:
            self._engine_ok = v

    # ── stop ────────────────────────────────
    def request_stop(self):
        self._stop.set()

    def should_stop(self):
        return self._stop.is_set()


# ─────────────────────────────────────────────
# Hilo: servidor TCP para el Engine
# ─────────────────────────────────────────────

def engine_server_thread(monitor_port, ws_id, state):
    """
    Abre un servidor TCP en monitor_port.
    Cuando el Engine conecta, responde a IDENTIFY con IDENTITY#<ws_id>
    y guarda el socket en state.engine_sock.
    Acepta reconexiones: si el Engine se cae y vuelve, lo registra de nuevo.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", monitor_port))
    srv.listen(1)
    srv.settimeout(1.0)   # para poder comprobar should_stop periódicamente
    print(f"[ENGINE-SRV] Escuchando al Engine en puerto {monitor_port}")

    while not state.should_stop():
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break

        print(f"[ENGINE-SRV] Engine conectado desde {addr}")

        # Protocolo: Engine envía IDENTIFY → respondemos IDENTITY#<ws_id>
        try:
            msg = recv_line(conn, timeout=5.0)
            if msg.strip() == "IDENTIFY":
                send_msg(conn, f"IDENTITY#{ws_id}")
                print(f"[ENGINE-SRV] Identidad enviada al Engine: {ws_id}")
                # Guardamos el socket; el ping_loop lo usará
                state.engine_sock = conn
                state.engine_ok   = True   # optimista hasta el primer PONG
            else:
                print(f"[ENGINE-SRV] Mensaje inesperado del Engine: {msg!r}")
                conn.close()
        except Exception as e:
            print(f"[ENGINE-SRV] Error en handshake con Engine: {e}")
            conn.close()

    srv.close()
    print("[ENGINE-SRV] Servidor de Engine cerrado.")


# ─────────────────────────────────────────────
# Hilo: bucle PING → Engine
# ─────────────────────────────────────────────

def ping_loop_thread(ws_id, state):
    """
    Cada PING_INTERVAL segundos envía PING#<n> al Engine y espera PONG#<n>#OK|KO.
    Actualiza state.engine_ok según la respuesta.
    """
    seq = 0
    while not state.should_stop():
        time.sleep(PING_INTERVAL)

        sock = state.engine_sock
        if sock is None:
            # Aún no ha conectado ningún Engine
            state.engine_ok = False
            continue

        seq += 1
        try:
            send_msg(sock, f"PING#{seq}")
            response = recv_line(sock, timeout=PING_TIMEOUT)

            # Esperamos PONG#<seq>#OK o PONG#<seq>#KO
            parts = response.strip().split("#")
            if len(parts) == 3 and parts[0] == "PONG" and parts[1] == str(seq):
                new_ok = (parts[2] == "OK")
                if state.engine_ok != new_ok:
                    estado = "OK ✓" if new_ok else "KO ✗"
                    print(f"[PING] Engine cambió a {estado}")
                state.engine_ok = new_ok
            else:
                print(f"[PING] Respuesta inesperada al PING#{seq}: {response!r}")
                state.engine_ok = False

        except socket.timeout:
            print(f"[PING] Timeout esperando PONG#{seq} → Engine KO")
            state.engine_ok = False
            # No cerramos el socket; quizás el Engine solo estaba lento.

        except (ConnectionError, OSError) as e:
            print(f"[PING] Socket del Engine caído: {e}")
            state.engine_ok = False
            state.engine_sock = None   # el engine_server_thread aceptará uno nuevo


# ─────────────────────────────────────────────
# Hilo: bucle HEALTH → Central
# ─────────────────────────────────────────────

def health_loop_thread(ws_id, central_sock, state):
    """
    Cada HEALTH_INTERVAL segundos envía HEALTH#<ws_id>#OK|KO a Central.
    """
    while not state.should_stop():
        time.sleep(HEALTH_INTERVAL)

        codigo = "OK" if state.engine_ok else "KO"
        msg    = f"HEALTH#{ws_id}#{codigo}"
        try:
            send_msg(central_sock, msg)
        except OSError as e:
            print(f"[HEALTH] No se pudo enviar latido a Central: {e}")
            state.request_stop()   # si perdemos Central, salimos
            break


# ─────────────────────────────────────────────
# Lectura de respuestas de Central (hilo main)
# ─────────────────────────────────────────────

def read_central_loop(central_sock, ws_id, state):
    """
    Lee cualquier mensaje que Central envíe de vuelta.
    Por ahora solo imprime (STATUS#OK#HEALTH_RECEIVED, etc.).
    """
    central_sock.settimeout(None)   # bloqueante
    try:
        while not state.should_stop():
            try:
                line = recv_line(central_sock, timeout=10.0)
                print(f"[CENTRAL] {line}")
            except socket.timeout:
                continue  # nada recibido, seguimos
    except (ConnectionError, OSError) as e:
        if not state.should_stop():
            print(f"[CENTRAL] Conexión perdida: {e}")
            state.request_stop()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="WM_WS_M: Monitor de Estación de Riego."
    )
    p.add_argument("--central-host",  required=True,  help="IP/host de Central")
    p.add_argument("--central-port",  type=int, required=True,  help="Puerto socket de Central")
    p.add_argument("--monitor-port",  type=int, required=True,  help="Puerto TCP que abre el Monitor para el Engine")
    p.add_argument("--ws-id",         required=True,  help="ID de la estación (ej: WS-01)")
    p.add_argument("--location",      required=True,  help="Ubicación (ej: Parque Central)")
    args = p.parse_args(argv)

    # Validaciones básicas
    for name, value in [("--ws-id", args.ws_id), ("--location", args.location)]:
        if not value.strip():
            p.error(f"{name} no puede estar vacío.")
        if any(c in value for c in ("#", "\n", "\r", "\x00")):
            p.error(f"{name} contiene un carácter no permitido.")

    if not 1 <= args.central_port <= 65535:
        p.error("--central-port fuera de rango.")
    if not 1 <= args.monitor_port <= 65535:
        p.error("--monitor-port fuera de rango.")

    return args


def main(argv=None):
    args  = parse_args(argv)
    state = MonitorState()

    # ── 1. Conectar a Central ────────────────
    print(f"[MAIN] Conectando a Central {args.central_host}:{args.central_port}…")
    try:
        central_sock = socket.create_connection(
            (args.central_host, args.central_port), timeout=10.0
        )
    except OSError as e:
        print(f"[ERROR] No se pudo conectar a Central: {e}")
        return 1

    # ── 2. Registro ──────────────────────────
    registro = f"REGISTRO#{args.ws_id}#{args.location}"
    try:
        send_msg(central_sock, registro)
        print(f"[MAIN] Enviado: {registro}")

        respuesta = recv_line(central_sock, timeout=10.0)
        print(f"[MAIN] Recibido: {respuesta}")

        partes = respuesta.strip().split("#")
        if not (len(partes) == 3 and partes[0] == "STATUS" and partes[1] == "OK"):
            print(f"[ERROR] Registro rechazado: {respuesta}")
            central_sock.close()
            return 1

        print(f"[MAIN] ✔ Estación {args.ws_id} registrada en Central.")

    except Exception as e:
        print(f"[ERROR] Durante el registro: {e}")
        central_sock.close()
        return 1

    # ── 3. Lanzar hilos ──────────────────────
    hilos = [
        threading.Thread(
            target=engine_server_thread,
            args=(args.monitor_port, args.ws_id, state),
            name="engine-srv", daemon=True
        ),
        threading.Thread(
            target=ping_loop_thread,
            args=(args.ws_id, state),
            name="ping-loop", daemon=True
        ),
        threading.Thread(
            target=health_loop_thread,
            args=(args.ws_id, central_sock, state),
            name="health-loop", daemon=True
        ),
        threading.Thread(
            target=read_central_loop,
            args=(central_sock, args.ws_id, state),
            name="central-reader", daemon=True
        ),
    ]

    for h in hilos:
        h.start()

    print("[MAIN] Monitor en marcha. Ctrl+C para detener.")

    # ── 4. Esperar señal de parada ───────────
    try:
        while not state.should_stop():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[MAIN] Interrupción de teclado, cerrando…")

    state.request_stop()
    central_sock.close()
    print("[MAIN] Monitor detenido.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())