"""Engine: mide y comunica el riego real, incluido el interrumpido."""
import argparse
import json
import os
import queue
import socket
import threading
import time
import uuid

from wm_common import (
    LineReader, messages, open_bus, publish, send_line,
)

FLOW = 15.0  # Litros/minuto de la simulacion.


def get_engine_state_file(station):
    return f"engine_state_{station}.json"


def save_engine_state(station, run, elapsed, volume, status="IN_PROGRESS"):
    state_file = get_engine_state_file(station)
    tmp_file = f"{state_file}.tmp"
    data = {
        "id": run["id"],
        "ws": station,
        "operator": run.get("operator"),
        "duration": run.get("duration"),
        "elapsed": round(elapsed, 3),
        "volume": round(volume, 4),
        "status": status,
        "timestamp": time.time(),
    }
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_file, state_file)
    except Exception as e:
        print(f"[PERSISTENCIA] Error guardando estado: {e}", flush=True)


def clear_engine_state(station):
    state_file = get_engine_state_file(station)
    try:
        if os.path.exists(state_file):
            os.remove(state_file)
    except OSError:
        pass


def load_engine_state(station):
    state_file = get_engine_state_file(station)
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


class Valve:
    def __init__(self, station, outbox, fault, link):
        self.station = station
        self.outbox = outbox
        self.fault = fault
        self.link = link
        self.lock = threading.RLock()
        self.current = None
        self.stop = threading.Event()
        self.reason = "PARADA_MANUAL"

    def event(self, kind, run, **data):
        self.outbox.put({
            "type": kind,
            "id": run["id"],
            "ws": self.station,
            **data,
        })

    def start(self, run):
        with self.lock:
            if self.current and self.current["id"] == run["id"]:
                return

            if (
                self.current
                or self.fault.is_set()
                or not self.link.is_set()
                or time.time() > run.get("expires", 0)
            ):
                self.event(
                    "REJECTED",
                    run,
                    reason="ENGINE_NO_DISPONIBLE",
                )
                return

            duration = run.get("duration")

            if (
                type(duration) is not int
                or not 1 <= duration <= 3600
            ):
                self.event(
                    "REJECTED", run, reason="DURACION_INVALIDA"
                )
                return

            self.current = run
            self.stop.clear()
            self.reason = "PARADA_MANUAL"

            threading.Thread(
                target=self.water,
                args=(run,),
                daemon=True,
            ).start()

    def close(self, reason, request_id=None):
        with self.lock:
            if self.current and (
                request_id is None
                or self.current["id"] == request_id
            ):
                self.reason = reason
                self.stop.set()

    def water(self, run):
        elapsed = volume = 0.0
        reason = "COMPLETADO"
        started = None

        try:
            self.event("STARTED", run)
            started = time.monotonic()
            save_engine_state(self.station, run, 0.0, 0.0, "IN_PROGRESS")

            print("[VALVULA] ABIERTA", flush=True)

            while True:
                remaining = (
                    run["duration"] - (time.monotonic() - started)
                )

                if remaining <= 0:
                    break

                # Event.wait permite interrumpir sin esperar un segundo entero.
                stopped = self.stop.wait(min(1.0, remaining))

                elapsed = min(
                    run["duration"], time.monotonic() - started
                )
                volume = FLOW * elapsed / 60
                save_engine_state(self.station, run, elapsed, volume, "IN_PROGRESS")

                if (
                    stopped
                    or self.fault.is_set()
                    or not self.link.is_set()
                ):
                    reason = (
                        "FUGA"
                        if self.fault.is_set()
                        else self.reason
                        if stopped
                        else "MONITOR_DESCONECTADO"
                    )
                    break

                self.event(
                    "TELEMETRY",
                    run,
                    flow=FLOW,
                    volume=round(volume, 4),
                    elapsed=round(elapsed, 3),
                )

                print(
                    f"[RIEGO] {elapsed:.1f}s, {volume:.2f} L",
                    flush=True,
                )

        except Exception as error:
            reason = "ERROR_KAFKA"
            print(f"[RIEGO] {error}", flush=True)

        finally:
            # La red nunca debe impedir el cierre de la valvula.
            if started is not None:
                elapsed = min(
                    run["duration"], time.monotonic() - started
                )
                volume = FLOW * elapsed / 60

            with self.lock:
                self.current = None

            print(
                f"[VALVULA] CERRADA: {reason}, {volume:.2f} L",
                flush=True,
            )

            try:
                self.event(
                    "END",
                    run,
                    flow=0,
                    volume=round(volume, 4),
                    elapsed=round(elapsed, 3),
                    reason=reason,
                )
                clear_engine_state(self.station)
            except Exception as error:
                print(
                    f"[FIN] No pudo notificarse: {error}",
                    flush=True,
                )


def sender(producer, outbox, stopped, valve):
    """La red va en otro hilo para no retrasar el cierre de la valvula."""
    pending = None

    while (
        not stopped.is_set()
        or pending is not None
        or not outbox.empty()
    ):
        try:
            if pending is None:
                pending = outbox.get(timeout=0.5)

            publish(producer, "ws_events", pending)
            pending = None

        except queue.Empty:
            continue

        except Exception as error:
            print(
                f"[EVENTOS] Reintentando: {error}",
                flush=True,
            )
            valve.close("ERROR_KAFKA")
            time.sleep(1)


def monitor_loop(
    args, station, link, fault, stopped, valve, initial
):
    current = initial

    while not stopped.is_set():
        sock = None

        try:
            if current is None:
                sock = socket.create_connection(
                    (args.monitor_host, args.monitor_port),
                    timeout=5,
                )

                reader = LineReader(sock)
                send_line(sock, "IDENTIFY")

                if reader.read() != f"IDENTITY#{station}":
                    raise ValueError(
                        "Monitor no disponible o identidad distinta"
                    )

            else:
                sock, reader = current
                current = None

            with sock:
                last_ping = time.monotonic()

                while not stopped.is_set():
                    try:
                        parts = reader.read(timeout=1).split("#")

                    except socket.timeout:
                        if time.monotonic() - last_ping > 3:
                            link.clear()
                            valve.close("MONITOR_SIN_LATIDOS")

                        continue

                    if (
                        len(parts) != 2
                        or parts[0] != "PING"
                        or not parts[1].isdigit()
                    ):
                        continue

                    last_ping = time.monotonic()
                    link.set()
                    code = "KO" if fault.is_set() else "OK"

                    send_line(
                        sock, f"PONG#{parts[1]}#{code}"
                    )

        except (OSError, ValueError, ConnectionError) as error:
            print(
                f"[MONITOR] Reconectando: {error}",
                flush=True,
            )

        finally:
            if sock is not None:
                sock.close()

            link.clear()
            valve.close("MONITOR_DESCONECTADO")

        stopped.wait(2)


def keyboard(
    station, producer, valve, fault, stopped
):
    print(
        "k=averia, r=reparar, m=pedir riego, "
        "p=parar riego, s=salir",
        flush=True,
    )

    while not stopped.is_set():
        try:
            option = input().strip().lower()

            if option == "k":
                fault.set()
                valve.close("FUGA")
                print("Averia activada", flush=True)

            elif option == "r":
                fault.clear()
                print("Averia reparada", flush=True)

            elif option == "p":
                valve.close("PARADA_MANUAL")

            elif option == "m":
                operator = (
                    input("Operario [FO-01]: ").strip()
                    or "FO-01"
                )

                duration = int(
                    input("Segundos [10]: ").strip()
                    or "10"
                )

                publish(producer, "fo_requests", {
                    "type": "REQUEST",
                    "id": uuid.uuid4().hex,
                    "operator": operator,
                    "ws": station,
                    "duration": duration,
                })

            elif option == "s":
                stopped.set()
                valve.close("APAGADO_ENGINE")

        except EOFError:
            return

        except Exception as error:
            print(f"[MENU] {error}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor-host", default="localhost")
    parser.add_argument("--monitor-port", type=int, required=True)
    parser.add_argument("--kafka-broker", required=True)
    args = parser.parse_args()

    # Preparar Kafka antes de responder OK a las comprobaciones de salud.
    producer, consumer = open_bus(
        args.kafka_broker, ("ws_orders",)
    )

    station = None

    while station is None:
        sock = None

        try:
            sock = socket.create_connection(
                (args.monitor_host, args.monitor_port),
                timeout=5,
            )

            reader = LineReader(sock)
            send_line(sock, "IDENTIFY")
            parts = reader.read().split("#")

            if len(parts) != 2 or parts[0] != "IDENTITY":
                raise ValueError("Monitor aun no registrado")

            station = parts[1]
            initial = (sock, reader)

        except (OSError, ValueError, ConnectionError) as error:
            if sock is not None:
                sock.close()

            print(f"[IDENTIDAD] {error}", flush=True)
            time.sleep(2)

    fault = threading.Event()
    link = threading.Event()
    stopped = threading.Event()

    outbox = queue.Queue()
    valve = Valve(station, outbox, fault, link)

    threading.Thread(
        target=sender,
        args=(producer, outbox, stopped, valve),
        daemon=True,
    ).start()

    threading.Thread(
        target=monitor_loop,
        args=(
            args, station, link, fault, stopped,
            valve, initial,
        ),
        daemon=True,
    ).start()

    threading.Thread(
        target=keyboard,
        args=(station, producer, valve, fault, stopped),
        daemon=True,
    ).start()

    print(f"[ENGINE] Identidad: {station}", flush=True)

    # Requisito de Resiliencia: si el Engine cayo durante un riego,
    # al recuperarse envia el estado final con los litros reales regados a medias.
    saved = load_engine_state(station)
    if saved and saved.get("status") == "IN_PROGRESS":
        print(
            f"[RECUPERACION] Detectado riego previo interrumpido por caída súbita: "
            f"ID={saved['id']}, Volumen={saved['volume']:.2f} L, Tiempo={saved['elapsed']:.1f} s",
            flush=True,
        )
        recovery_event = {
            "type": "END",
            "id": saved["id"],
            "ws": station,
            "flow": 0.0,
            "volume": float(saved["volume"]),
            "elapsed": float(saved["elapsed"]),
            "reason": "INTERRUMPIDO_POR_CAIDA_ENGINE",
            "operator": saved.get("operator"),
        }
        try:
            publish(producer, "ws_events", recovery_event)
            print("[RECUPERACION] Estado final del riego enviado a CENTRAL vía Kafka", flush=True)
        except Exception as err:
            print(f"[RECUPERACION] Error enviando estado final a Kafka: {err}", flush=True)
        clear_engine_state(station)

    try:
        while not stopped.is_set():
            try:
                for message in messages(consumer):
                    if message.get("ws") != station:
                        continue

                    if message.get("type") == "START":
                        valve.start(message)

                    elif message.get("type") == "STOP":
                        valve.close(
                            message.get("reason", "CENTRAL"),
                            message.get("id"),
                        )

            except Exception as error:
                print(f"[KAFKA] {error}", flush=True)
                valve.close("ERROR_KAFKA")
                stopped.wait(1)

    except KeyboardInterrupt:
        pass

    finally:
        stopped.set()
        valve.close("APAGADO_ENGINE")

        # Dar tiempo a notificar el cierre antes de terminar.
        time.sleep(2)
        consumer.close()


if __name__ == "__main__":
    main()