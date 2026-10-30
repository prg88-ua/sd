"""Monitor: conserva los mensajes de sockets definidos en el contrato."""
import argparse
import socket
import threading
import time

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from wm_common import LineReader, field, send_line


class Monitor:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.registered = threading.Event()
        self.engine = None
        self.last_ok = 0.0

    def healthy(self):
        with self.lock:
            return (
                self.engine is not None
                and time.monotonic() - self.last_ok < 3
            )

    def engine_session(self, sock):
        reader = LineReader(sock)

        try:
            if reader.read() != "IDENTIFY":
                send_line(sock, "STATUS#ERROR#INVALID_MESSAGE")
                return

            with self.lock:
                if self.engine is not None:
                    send_line(sock, "STATUS#ERROR#DUPLICATE_ENGINE")
                    return

                self.engine = sock

                # Identificarse no demuestra que el Engine este sano.
                self.last_ok = 0

            send_line(sock, f"IDENTITY#{self.args.ws_id}")
            seq = 0

            while True:
                with self.lock:
                    if self.engine is not sock:
                        break

                started = time.monotonic()
                seq += 1
                send_line(sock, f"PING#{seq}")

                deadline = started + 2
                ok = False

                while time.monotonic() < deadline:
                    try:
                        parts = reader.read(
                            max(0.01, deadline - time.monotonic())
                        ).split("#")

                    except socket.timeout:
                        break

                    if (
                        len(parts) == 3
                        and parts[:2] == ["PONG", str(seq)]
                        and parts[2] in ("OK", "KO")
                    ):
                        ok = parts[2] == "OK"
                        break

                    # Un PONG atrasado no vale para el PING actual.

                with self.lock:
                    self.last_ok = time.monotonic() if ok else 0

                time.sleep(
                    max(0, 1 - (time.monotonic() - started))
                )

        except (OSError, ValueError, ConnectionError) as error:
            print(f"[ENGINE] {error}", flush=True)

        finally:
            with self.lock:
                if self.engine is sock:
                    self.engine = None
                    self.last_ok = 0

            sock.close()

    def engine_server(self):
        with socket.socket() as server:
            server.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )

            server.bind(("0.0.0.0", self.args.monitor_port))
            server.listen()

            while True:
                sock, _ = server.accept()

                threading.Thread(
                    target=self.engine_session,
                    args=(sock,),
                    daemon=True,
                ).start()

    def run(self):
        # Al arrancar todavía no hay una comprobación válida del Engine.
        # Conservar el último estado fuera de la reconexión evita repetirlo
        # al recuperar Central: la consola solo muestra cambios reales.
        last_reported_health = "KO"
        print("[SALUD] KO", flush=True)

        threading.Thread(
            target=self.engine_server, daemon=True
        ).start()

        while True:
            try:
                with socket.create_connection(
                    (
                        self.args.central_host,
                        self.args.central_port,
                    ),
                    timeout=5,
                ) as sock:
                    reader = LineReader(sock)

                    send_line(
                        sock,
                        f"REGISTRO#{self.args.ws_id}#{self.args.location}",
                    )

                    reg_response = reader.read()
                    if reg_response != "STATUS#OK#REGISTERED":
                        raise ValueError(
                            f"Registro rechazado por CENTRAL: {reg_response}"
                        )

                    self.registered.set()
                    print("[CENTRAL] Registro aceptado", flush=True)

                    while True:
                        started = time.monotonic()
                        code = "OK" if self.healthy() else "KO"

                        send_line(
                            sock,
                            f"HEALTH#{self.args.ws_id}#{code}",
                        )

                        if reader.read() != "STATUS#OK#HEALTH_RECEIVED":
                            raise ValueError(
                                "Respuesta HEALTH incorrecta"
                            )

                        # HEALTH sigue siendo periódico para detectar caídas;
                        # lo que evitamos es imprimir el mismo estado sin parar.
                        if code != last_reported_health:
                            print(f"[SALUD] {code}", flush=True)
                            last_reported_health = code

                        time.sleep(
                            max(0, 1 - (time.monotonic() - started))
                        )

            except (OSError, ValueError, ConnectionError) as error:
                print(
                    f"[CENTRAL] Reconectando: {error}",
                    flush=True,
                )

            finally:
                self.registered.clear()
                # NO cortar la conexion con el Engine al perder Central.
                # El requisito exige que la estacion siga prestando servicio
                # y termine de regar sus litros programados aunque Central caiga.

            time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--central-host", required=True)
    parser.add_argument("--central-port", type=int, required=True)
    parser.add_argument("--monitor-port", type=int, required=True)
    parser.add_argument("--ws-id", required=True)
    parser.add_argument("--location", required=True)
    args = parser.parse_args()

    if not field(args.ws_id) or not field(args.location):
        parser.error(
            "ID y ubicacion no pueden contener # ni saltos de linea"
        )

    if not all(
        1 <= p <= 65535
        for p in (args.central_port, args.monitor_port)
    ):
        parser.error("Puertos fuera de rango")

    try:
        Monitor(args).run()
    except KeyboardInterrupt:
        print("Monitor detenido")


if __name__ == "__main__":
    main()
