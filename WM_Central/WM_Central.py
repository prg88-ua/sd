"""CENTRAL: sockets para monitores, Kafka para riegos y panel web."""
import argparse
import queue
import socket
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from database import connection, init_db
from wm_common import (
    LineReader, field, messages, open_bus, publish, send_line,
)


class Central:
    def __init__(self):
        # Todas las decisiones sobre estaciones se protegen con este bloqueo.
        self.lock = threading.RLock()
        self.outbox = queue.Queue()
        self.ready = threading.Event()
        self.monitors = {}
        self.active = {}
        self.seen = set()

        with connection() as conn:
            self.stations = {
                r["id"]: dict(r)
                for r in conn.execute("SELECT * FROM watering_stations")
            }

        # Tras reiniciar no conocemos la salud real hasta recibir al Monitor.
        for ws in self.stations.values():
            ws.update(live=False, health=None, seen=0)
            ws["current_operator"] = None
            ws["current_flow"] = 0
            self.save(ws)

    def status(self, ws):
        if not ws["live"] or time.monotonic() - ws["seen"] > 5:
            return "OFFLINE"

        if ws["health"] is None:
            return "OFFLINE"

        if ws["health"] != "OK":
            return "LEAK"

        if ws["blocked"]:
            return "OUT_OF_SERVICE"

        run = self.active.get(ws["id"])

        return "WATERING" if run and run["started"] else "AVAILABLE"

    def save(self, ws):
        ws["status"] = self.status(ws)

        with connection() as conn:
            conn.execute("""
                UPDATE watering_stations
                SET location=?, status=?, current_flow=?,
                    accumulated_volume=?, current_operator=?,
                    last_health_check=?, blocked=?
                WHERE id=?
            """, (
                ws["location"],
                ws["status"],
                ws["current_flow"],
                ws["accumulated_volume"],
                ws["current_operator"],
                ws["last_health_check"],
                int(ws["blocked"]),
                ws["id"],
            ))

    def snapshot(self):
        with self.lock:
            keys = (
                "id", "location", "status", "current_flow",
                "accumulated_volume", "current_operator",
            )

            return [
                dict((k, ws[k]) for k in keys)
                | {"status": self.status(ws)}
                for ws in self.stations.values()
            ]

    def emit(self, topic, message):
        # El hilo Kafka hace la red. Un envio lento no bloquea los latidos.
        self.outbox.put((topic, message))

    def reply(self, original, kind, **data):
        self.emit("fo_responses", {
            "type": kind,
            "id": original["id"],
            "operator": original.get("operator"),
            "ws": original.get("ws"),
            **data,
        })

    def finish(self, ws_id, reason):
        """Finaliza con la ultima medida real recibida del Engine."""
        run = self.active.pop(ws_id, None)

        if run is None:
            return

        ws = self.stations[ws_id]
        ws["current_flow"] = 0
        ws["current_operator"] = None
        ws["accumulated_volume"] = run["volume"]

        with connection() as conn:
            conn.execute("""
                INSERT INTO irrigation_history
                (ws_id,operator_id,start_time,end_time,total_volume)
                VALUES (?,?,?,?,?)
            """, (
                ws_id,
                run["operator"],
                run["start_time"],
                datetime.now().isoformat(),
                run["volume"],
            ))

        self.save(ws)

        self.reply(
            run,
            "END",
            volume=run["volume"],
            elapsed=run["elapsed"],
            reason=reason,
        )

        print(
            f"[FIN] {ws_id}: {reason}, {run['volume']:.2f} L",
            flush=True,
        )

    def stop_order(self, ws_id, reason):
        run = self.active.get(ws_id)

        if run:
            self.emit("ws_orders", {
                "type": "STOP",
                "id": run["id"],
                "ws": ws_id,
                "reason": reason,
            })

    def handle_request(self, message):
        with self.lock:
            if not all(
                field(message.get(k)) for k in ("id", "operator")
            ):
                return

            with connection() as conn:
                operator_ok = conn.execute(
                    "SELECT 1 FROM operators WHERE id=?",
                    (message["operator"],),
                ).fetchone()

            if message["operator"] != "CENTRAL" and not operator_ok:
                self.reply(
                    message, "DENIED", reason="OPERARIO_NO_REGISTRADO"
                )
                return

            if message.get("type") == "LIST":
                self.reply(
                    message, "STATIONS", stations=self.snapshot()
                )
                return

            if (
                message.get("type") != "REQUEST"
                or not field(message.get("ws"))
            ):
                self.reply(
                    message, "DENIED", reason="PETICION_INVALIDA"
                )
                return

            if message["id"] in self.seen:
                return

            self.seen.add(message["id"])

            ws_id = message["ws"]
            duration = message.get("duration")
            ws = self.stations.get(ws_id)
            reason = None

            if type(duration) is not int or not 1 <= duration <= 3600:
                reason = "DURACION_INVALIDA"
            elif ws is None:
                reason = "ESTACION_NO_EXISTE"
            elif self.status(ws) != "AVAILABLE" or ws_id in self.active:
                reason = "ESTACION_NO_DISPONIBLE"

            if reason:
                self.reply(message, "DENIED", reason=reason)
                return

            # Reservamos antes de enviar START: otra peticion no puede colarse.
            run = dict(
                message,
                started=False,
                volume=0.0,
                elapsed=0.0,
                last_event=time.monotonic(),
                start_time=datetime.now().isoformat(),
            )

            self.active[ws_id] = run
            ws["accumulated_volume"] = 0
            ws["current_operator"] = message["operator"]
            self.save(ws)

            self.reply(
                message,
                "PENDING",
                reason="ESPERANDO_CONFIRMACION_ENGINE",
            )

            self.emit(
                "ws_orders",
                dict(message, type="START", expires=time.time() + 12),
            )

            print(f"[SOLICITUD] {message}", flush=True)

    def handle_event(self, message):
        with self.lock:
            ws_id = message.get("ws")

            if not isinstance(ws_id, str):
                return

            run = self.active.get(ws_id)

            if not run or message.get("id") != run["id"]:
                return  # No aplicar un evento de un riego anterior.

            ws = self.stations[ws_id]
            kind = message.get("type")

            if kind == "REJECTED":
                self.finish(
                    ws_id, message.get("reason", "RECHAZADO_ENGINE")
                )

            elif kind == "STARTED":
                run["started"] = True
                run["start_time"] = datetime.now().isoformat()
                run["last_event"] = time.monotonic()

                self.save(ws)
                self.reply(run, "ACK", reason="RIEGO_INICIADO")

            elif kind in ("TELEMETRY", "END"):
                volume = float(message["volume"])
                elapsed = float(message["elapsed"])
                flow = float(message.get("flow", 0))

                # Rechazar medidas negativas, infinitas o decrecientes.
                if not (
                    run["volume"] <= volume <= 1000000
                    and run["elapsed"] <= elapsed <= 7200
                    and 0 <= flow <= 10000
                ):
                    return

                run.update(
                    volume=volume,
                    elapsed=elapsed,
                    last_event=time.monotonic(),
                )

                ws["accumulated_volume"] = volume
                ws["current_flow"] = flow

                if kind == "END":
                    if message.get("reason") == "FUGA":
                        ws["health"] = "KO"

                    self.finish(
                        ws_id, message.get("reason", "COMPLETADO")
                    )

                else:
                    self.save(ws)
                    self.reply(
                        run,
                        "UPDATE",
                        volume=volume,
                        elapsed=elapsed,
                        flow=flow,
                    )

    def admin(self, action, target, duration=10):
        with self.lock:
            ids = (
                list(self.stations)
                if target == "ALL"
                else [target]
            )

            if any(i not in self.stations for i in ids):
                return {"error": "ESTACION_NO_EXISTE"}, 404

            if action == "start":
                if (
                    type(duration) is not int
                    or not 1 <= duration <= 3600
                ):
                    return {"error": "DURACION_INVALIDA"}, 400

                for ws_id in ids:
                    self.handle_request({
                        "type": "REQUEST",
                        "id": uuid.uuid4().hex,
                        "operator": "CENTRAL",
                        "ws": ws_id,
                        "duration": duration,
                    })

            elif action in ("block", "activate"):
                for ws_id in ids:
                    ws = self.stations[ws_id]
                    ws["blocked"] = action == "block"

                    if action == "block":
                        self.stop_order(ws_id, "BLOQUEADA")

                    self.save(ws)

            else:
                return {"error": "ACCION_INVALIDA"}, 400

            return {"status": "OK"}, 200

    def client(self, sock):
        ws_id = None
        reader = LineReader(sock)

        try:
            while True:
                try:
                    line = reader.read(timeout=6)
                except socket.timeout:
                    break

                parts = line.split("#")

                with self.lock:
                    if (
                        len(parts) == 3
                        and parts[0] == "REGISTRO"
                        and ws_id is None
                        and field(parts[1])
                        and field(parts[2])
                    ):
                        candidate, location = parts[1:]

                        if candidate in self.monitors:
                            send_line(
                                sock,
                                "STATUS#ERROR#DUPLICATE_STATION",
                            )
                            continue

                        ws_id = candidate

                        with connection() as conn:
                            conn.execute("""
                                INSERT OR IGNORE INTO watering_stations
                                (id,location) VALUES (?,?)
                            """, (ws_id, location))

                            row = conn.execute(
                                "SELECT * FROM watering_stations WHERE id=?",
                                (ws_id,),
                            ).fetchone()

                        ws = self.stations.setdefault(ws_id, dict(row))

                        ws.update(
                            location=location,
                            live=True,
                            health=None,
                            seen=time.monotonic(),
                        )

                        self.monitors[ws_id] = sock
                        self.save(ws)
                        send_line(sock, "STATUS#OK#REGISTERED")

                    elif (
                        len(parts) == 3
                        and parts[0] == "HEALTH"
                        and parts[1] == ws_id
                        and ws_id is not None
                        and parts[2] in ("OK", "KO")
                    ):
                        ws = self.stations[ws_id]

                        ws.update(
                            health=parts[2],
                            seen=time.monotonic(),
                            last_health_check=datetime.now().isoformat(),
                        )

                        if parts[2] == "KO":
                            self.stop_order(ws_id, "FUGA")

                        # Un bloqueo no impide renovar el ultimo latido.
                        self.save(ws)
                        send_line(sock, "STATUS#OK#HEALTH_RECEIVED")

                    else:
                        send_line(sock, "STATUS#ERROR#INVALID_MESSAGE")

        except (OSError, ConnectionError, ValueError) as error:
            print(f"[SOCKET] {error}", flush=True)

        finally:
            with self.lock:
                if ws_id and self.monitors.get(ws_id) is sock:
                    self.monitors.pop(ws_id)
                    ws = self.stations[ws_id]
                    ws["live"] = False

                    self.stop_order(ws_id, "DESCONECTADA")
                    self.finish(ws_id, "DESCONECTADA_ULTIMA_MEDIDA")
                    self.save(ws)

            sock.close()

    def sockets(self, port):
        with socket.socket() as server:
            server.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            server.bind(("0.0.0.0", port))
            server.listen()

            print(f"[SOCKET] Escuchando en {port}", flush=True)

            while True:
                sock, _ = server.accept()

                threading.Thread(
                    target=self.client,
                    args=(sock,),
                    daemon=True,
                ).start()

    def watchdog(self):
        while True:
            time.sleep(0.5)

            with self.lock:
                for ws_id, ws in list(self.stations.items()):
                    if (
                        ws["live"]
                        and time.monotonic() - ws["seen"] > 5
                    ):
                        ws["live"] = False
                        sock = self.monitors.get(ws_id)

                        if sock:
                            try:
                                sock.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass

                        self.stop_order(ws_id, "DESCONECTADA")
                        self.finish(
                            ws_id, "DESCONECTADA_ULTIMA_MEDIDA"
                        )
                        self.save(ws)

                for ws_id, run in list(self.active.items()):
                    limit = 6 if run["started"] else 12

                    if time.monotonic() - run["last_event"] > limit:
                        self.stop_order(
                            ws_id, "SIN_RESPUESTA_ENGINE"
                        )
                        self.finish(
                            ws_id, "SIN_RESPUESTA_ULTIMA_MEDIDA"
                        )

    def kafka(self, broker):
        while True:
            producer, consumer = open_bus(
                broker, ("fo_requests", "ws_events")
            )

            self.ready.set()
            print("[KAFKA] CENTRAL preparada", flush=True)
            pending = None

            try:
                while True:
                    for _ in range(50):
                        if pending is None:
                            try:
                                pending = self.outbox.get_nowait()
                            except queue.Empty:
                                break

                        publish(producer, *pending)
                        pending = None

                    for message in messages(consumer):
                        try:
                            if message.get("type") in ("REQUEST", "LIST"):
                                self.handle_request(message)
                            else:
                                self.handle_event(message)

                        except (ValueError, TypeError, KeyError) as error:
                            print(
                                f"[MENSAJE INVALIDO] {error}",
                                flush=True,
                            )

            except Exception as error:
                print(f"[KAFKA] Reconectando: {error}", flush=True)

                if pending is not None:
                    self.outbox.put(pending)

            finally:
                self.ready.clear()
                consumer.close()
                producer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket-port", type=int, required=True)
    parser.add_argument("--kafka-broker", required=True)
    args = parser.parse_args()

    init_db()
    central = Central()

    for target, values in (
        (central.kafka, (args.kafka_broker,)),
        (central.sockets, (args.socket_port,)),
        (central.watchdog, ()),
    ):
        threading.Thread(
            target=target, args=values, daemon=True
        ).start()

    app = Flask(__name__)
    dashboard = Path(__file__).resolve().parents[1] / "WM_Dashboard"

    @app.get("/")
    @app.get("/index.html")
    def index():
        return send_from_directory(dashboard, "index.html")

    @app.get("/health")
    def health():
        ready = central.ready.is_set()
        return jsonify({"kafka": ready}), 200 if ready else 503

    @app.get("/api/stations")
    def stations():
        return jsonify(central.snapshot())

    @app.post("/api/stations/<ws_id>/<action>")
    def action(ws_id, action):
        body = request.get_json(silent=True) or {}

        result, code = central.admin(
            action, ws_id, body.get("duration", 10)
        )

        return jsonify(result), code

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()