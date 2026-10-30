"""CENTRAL: sockets para monitores, Kafka para riegos y panel web."""
import argparse
import json
import math
<<<<<<< HEAD
import re
=======
import os
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
import socket
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask, jsonify, request, send_from_directory

from WM_Central.database import connection, init_db
<<<<<<< HEAD
from WM_Central.logs import CentralLogs
=======
from wm_state import safe_id
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

from wm_common import (
    LineReader, field, open_bus, publish, send_line,
)


class Central:
    def __init__(self):
<<<<<<< HEAD
        self.logs = CentralLogs()
=======
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
        self.lock = threading.RLock()
        self.ready = threading.Event()
        self.monitors = {}
        self.recovering = True
        with connection() as conn:
            self.stations = {
                r["id"]: dict(r) for r in conn.execute("SELECT * FROM watering_stations")
            }
        self.restore_requests()
        for ws in self.stations.values():
            # Reiniciar Central no demuestra que Monitor y Engine estén vivos.
            ws.update(live=False, health=None, seen=0)
            run = self.active.get(ws["id"])
            ws["current_operator"] = run["operator"] if run else None
            ws["current_flow"] = 0
            self.save(ws)
        self.log("[CENTRAL] Estado recuperado. Esperando conexiones de las estaciones.")

    def log(self, message, level="INFO"):
        """El mismo evento se conserva para el panel y sigue saliendo en Docker."""
        entry = self.logs.append(message, level)
        print(entry["message"], flush=True)

    def restore_requests(self):
        """La memoria es una caché; SQLite conserva la verdad tras una caída."""
        self.active = {}
        self.last_completed_by_operator = {}
        with connection() as conn:
            rows = conn.execute("SELECT status,payload FROM requests").fetchall()
            for row in conn.execute("SELECT * FROM watering_stations"):
                self.stations[row["id"]].update(dict(row))
        for row in rows:
            run = json.loads(row["payload"])
            if row["status"] == "ACTIVE":
                run["last_event"] = time.monotonic()
                self.active[run["ws"]] = run
            elif row["status"] in ("FINAL", "PROVISIONAL"):
                self.remember_result(run)

    def remember_result(self, result):
        previous = self.last_completed_by_operator.get(result.get("operator"))
        # Una corrección tardía no sustituye el resultado de un riego más reciente.
        if not previous or result["timestamp"] >= previous["timestamp"]:
            self.last_completed_by_operator[result["operator"]] = result

    def store_request(self, run, status):
        with connection() as conn:
            conn.execute("""
                INSERT INTO requests VALUES (?,?,?)
                ON CONFLICT(request_id) DO UPDATE
                SET status=excluded.status,payload=excluded.payload
            """, (run["id"], status, json.dumps(run)))

    def load_request(self, request_id):
        with connection() as conn:
            row = conn.execute("SELECT status,payload FROM requests WHERE request_id=?",
                               (request_id,)).fetchone()
        return (row["status"], json.loads(row["payload"])) if row else None

    def register_catalog(self, payload):
        """Da de alta identidades sin modificar un riego ni una fila existente.

        Validamos TODO el lote antes de abrir la transacción. El mismo bloqueo
        que usan Kafka y los monitores protege también la caché de estaciones;
        así una identidad nueva está disponible sin reiniciar Central.
        """
        if not isinstance(payload, dict) or set(payload) - {"operators", "stations"}:
            raise ValueError("El catálogo debe contener operators y/o stations")
        catalog = {}
        for kind, label in (("operators", "name"), ("stations", "location")):
            entries = payload.get(kind, [])
            if not isinstance(entries, list) or len(entries) > 1000:
                raise ValueError(f"{kind} debe ser una lista de hasta 1000 elementos")
            normalized = []
            identities = set()
            for entry in entries:
                if not isinstance(entry, dict) or set(entry) != {"id", label}:
                    raise ValueError(f"Cada elemento de {kind} necesita id y {label}")
                identity = safe_id(entry["id"])
                if len(identity) > 80:
                    raise ValueError("El identificador no puede superar 80 caracteres")
                text = entry[label]
                if not isinstance(text, str) or not field(text) or len(text) > 200:
                    raise ValueError(f"{label} debe tener entre 1 y 200 caracteres sin # ni saltos")
                if identity in identities:
                    raise ValueError(f"ID repetido dentro de {kind}: {identity}")
                identities.add(identity)
                normalized.append((identity, text))
            catalog[kind] = normalized

        created = {"operators": 0, "stations": 0}
        with self.lock:
            pending_stations = {}
            with connection() as conn:
                for identity, name in catalog["operators"]:
                    cursor = conn.execute(
                        "INSERT OR IGNORE INTO operators(id,name) VALUES (?,?)",
                        (identity, name),
                    )
                    created["operators"] += cursor.rowcount
                for identity, location in catalog["stations"]:
                    cursor = conn.execute(
                        "INSERT OR IGNORE INTO watering_stations(id,location) VALUES (?,?)",
                        (identity, location),
                    )
                    created["stations"] += cursor.rowcount
                    if identity not in self.stations:
                        row = conn.execute(
                            "SELECT * FROM watering_stations WHERE id=?", (identity,)
                        ).fetchone()
                        station = dict(row)
                        station.update(live=False, health=None, seen=0)
                        pending_stations[identity] = station
            # Publicar en memoria SOLO después de confirmar SQLite. Ante error,
            # no queda una estación fantasma que la BD haya deshecho.
            self.stations.update(pending_stations)
        return {
            "created": created,
            "operators": [identity for identity, _ in catalog["operators"]],
            "stations": [identity for identity, _ in catalog["stations"]],
        }

    def restore_requests(self):
        """La memoria es una caché; SQLite conserva la verdad tras una caída."""
        self.active = {}
        self.last_completed_by_operator = {}
        with connection() as conn:
            rows = conn.execute("SELECT status,payload FROM requests").fetchall()
            for row in conn.execute("SELECT * FROM watering_stations"):
                self.stations[row["id"]].update(dict(row))
        for row in rows:
            run = json.loads(row["payload"])
            if row["status"] == "ACTIVE":
                run["last_event"] = time.monotonic()
                self.active[run["ws"]] = run
            elif row["status"] in ("FINAL", "PROVISIONAL"):
                self.remember_result(run)

    def remember_result(self, result):
        previous = self.last_completed_by_operator.get(result.get("operator"))
        # Una corrección tardía no sustituye el resultado de un riego más reciente.
        if not previous or result["timestamp"] >= previous["timestamp"]:
            self.last_completed_by_operator[result["operator"]] = result

    def store_request(self, run, status):
        with connection() as conn:
            conn.execute("""
                INSERT INTO requests VALUES (?,?,?)
                ON CONFLICT(request_id) DO UPDATE
                SET status=excluded.status,payload=excluded.payload
            """, (run["id"], status, json.dumps(run)))

    def load_request(self, request_id):
        with connection() as conn:
            row = conn.execute("SELECT status,payload FROM requests WHERE request_id=?",
                               (request_id,)).fetchone()
        return (row["status"], json.loads(row["payload"])) if row else None

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
        previous_status = ws.get("status")
        ws["status"] = self.status(ws)

        with connection() as conn:
            conn.execute("""
                UPDATE watering_stations
                SET location=?, status=?, current_flow=?,
                    accumulated_volume=?, current_operator=?,
                    last_health_check=?, blocked=?, last_request_id=?
                WHERE id=?
            """, (
                ws["location"],
                ws["status"],
                ws["current_flow"],
                ws["accumulated_volume"],
                ws["current_operator"],
                ws["last_health_check"],
                int(ws["blocked"]),
                ws.get("last_request_id"),
                ws["id"],
            ))

        if ws["id"] in self.active:
            self.store_request(self.active[ws["id"]], "ACTIVE")
<<<<<<< HEAD
        # save se llama también en cada HEALTH: registrar solo transiciones
        # evita llenar la terminal de OK repetidos y de medidas idénticas.
        if previous_status != ws["status"]:
            level = "WARNING" if ws["status"] in ("LEAK", "OFFLINE") else "INFO"
            self.log(f"[ESTADO] {ws['id']}: {previous_status} -> {ws['status']}", level)
=======
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

    def snapshot(self):
        with self.lock, connection():
            keys = (
                "id", "location", "status", "current_flow",
                "accumulated_volume", "current_operator",
            )

            return [
                dict((k, ws[k]) for k in keys)
                | {"status": self.status(ws)}
                for ws in self.stations.values()
            ]

    def register_catalog(self, body):
        """Dar de alta IDs desde el lanzador sin reiniciar ni borrar historial."""
        if not isinstance(body, dict) or set(body) - {"operators", "stations"}:
            raise ValueError("Catálogo inválido")
        groups = []
        # Validar TODO antes de escribir: un lote incorrecto no deja medias altas.
        for key, prefix, label in (("operators", "FO", "name"),
                                   ("stations", "WS", "location")):
            rows = body.get(key, [])
            if not isinstance(rows, list) or len(rows) > 500:
                raise ValueError("Máximo 500 altas por lote")
            for row in rows:
                if (not isinstance(row, dict)
                        or not isinstance(row.get("id"), str)
                        or not re.fullmatch(prefix + r"-[0-9]{2,6}", row["id"])
                        or int(row["id"].split("-")[1]) < 1
                        or not field(row.get(label)) or len(row[label]) > 100):
                    raise ValueError("ID o descripción inválidos")
            groups.append((key, label, rows))
        with self.lock:
            with connection() as conn:
                for key, label, rows in groups:
                    table = "operators" if key == "operators" else "watering_stations"
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {table}(id,{label}) VALUES (?,?)",
                        [(row["id"], row[label]) for row in rows],
                    )
                stations = [dict(row) for row in conn.execute("SELECT * FROM watering_stations")]
            # Actualizar la caché después del commit, sin pisar las estaciones vivas.
            for row in stations:
                if row["id"] not in self.stations:
                    row.update(live=False, health=None, seen=0)
                    self.stations[row["id"]] = row
        return {"ok": True}

    def emit(self, topic, message):
        # Outbox persistente: si caemos antes de publicar, se enviará al reiniciar.
        with connection() as conn:
            conn.execute("INSERT INTO central_outbox(topic,payload) VALUES (?,?)",
                         (topic, json.dumps(message)))

    def reply(self, original, kind, **data):
        self.emit("fo_responses", dict(
            type=kind, id=original["id"], operator=original.get("operator"),
            ws=original.get("ws"), **data,
        ))

    def finalize(self, run, reason, provisional=False):
        ws_id = run["ws"]
        result = dict(run, reason=reason,
                      timestamp=run.get("timestamp", time.time()),
                      updated_at=time.time(), final=not provisional)
        self.store_request(result, "PROVISIONAL" if provisional else "FINAL")
        with connection() as conn:
            # Una fila por petición, incluso si un END se retransmite varias veces.
            conn.execute("""
                INSERT INTO irrigation_history
                (request_id,ws_id,operator_id,start_time,end_time,total_volume)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(request_id) DO UPDATE
                SET end_time=excluded.end_time,total_volume=excluded.total_volume
            """, (run["id"], ws_id, run["operator"], run.get("start_time"),
                  datetime.now().isoformat(), run["volume"]))
        self.remember_result(result)
        ws = self.stations[ws_id]
        # Un END antiguo nunca modifica las medidas de otra sesión.
        if ws.get("last_request_id") == run["id"]:
            ws.update(current_flow=0, current_operator=None,
                      accumulated_volume=run["volume"])
            self.save(ws)
        self.reply(result, "END", volume=run["volume"], elapsed=run["elapsed"],
                   reason=reason, final=not provisional, updated_at=result["updated_at"])
<<<<<<< HEAD
        self.log(f"[FIN] {ws_id}: {reason}, {run['volume']:.2f} L")
=======
        print(f"[FIN] {ws_id}: {reason}, {run['volume']:.2f} L", flush=True)
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

    def finish(self, ws_id, reason):
        with self.lock, connection():
            run = self.active.pop(ws_id, None)
            if run:
                self.finalize(run, reason, provisional="ULTIMA_MEDIDA" in reason)

    def stop_order(self, ws_id, reason):
        run = self.active.get(ws_id)
        if run:
            self.emit("ws_orders", dict(type="STOP", id=run["id"],
                                       ws=ws_id, reason=reason))

    def handle_request(self, message):
        with self.lock, connection():
            if not all(field(message.get(k)) for k in ("id", "operator")):
                return
            with connection() as conn:
                operator_ok = conn.execute("SELECT 1 FROM operators WHERE id=?",
                                           (message["operator"],)).fetchone()
            if message["operator"] != "CENTRAL" and not operator_ok:
                self.reply(message, "DENIED", reason="OPERARIO_NO_REGISTRADO")
                return
            if message.get("type") == "LIST":
                op = message["operator"]
                active = next((dict(r) for r in self.active.values()
                               if r["operator"] == op), None)
                completed = self.last_completed_by_operator.get(op)
                wanted = self.load_request(message.get("recover_id"))
                if wanted and wanted[1].get("operator") == op:
                    if wanted[0] == "ACTIVE":
                        active = wanted[1]
                    elif wanted[0] in ("FINAL", "PROVISIONAL", "DENIED"):
                        completed = wanted[1]
                self.reply(message, "STATIONS", stations=self.snapshot(),
                           active_order=active, last_completed=completed)
                return
            if message.get("type") != "REQUEST" or not field(message.get("ws")):
                self.reply(message, "DENIED", reason="PETICION_INVALIDA")
                return
            previous = self.load_request(message["id"])
            if previous:
                status, run = previous
                if run["operator"] != message["operator"] or run["ws"] != message["ws"]:
                    self.reply(message, "DENIED", reason="ID_PETICION_REUTILIZADO")
                elif status == "DENIED":
                    self.reply(run, "DENIED", reason=run["reason"])
                elif status in ("FINAL", "PROVISIONAL"):
                    self.reply(run, "END", volume=run["volume"], elapsed=run["elapsed"],
                               reason=run["reason"], final=run.get("final", True),
                               updated_at=run["updated_at"])
                else:
                    self.reply(run, "ACK" if run["started"] else "PENDING",
                               reason="PETICION_YA_REGISTRADA")
                return
            ws_id = message["ws"]
            duration = message.get("duration")
            submitted = message.get("submitted_at")
            ws = self.stations.get(ws_id)
            reason = None
<<<<<<< HEAD
            # Los PCs pueden tener horas distintas. La fecha es informativa;
            # la identidad persistente de la petición evita ejecutar duplicados.
            if type(duration) is not int or not 1 <= duration <= 3600:
=======
            if not isinstance(submitted, (int, float)) or not math.isfinite(submitted) or not -5 <= time.time()-submitted <= 60:
                # Evita ejecutar pedidos históricos al migrar o vaciar los offsets.
                reason = "PETICION_CADUCADA_O_SIN_FECHA"
            elif type(duration) is not int or not 1 <= duration <= 3600:
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
                reason = "DURACION_INVALIDA"
            elif ws is None:
                reason = "ESTACION_NO_EXISTE"
            elif self.status(ws) != "AVAILABLE" or ws_id in self.active:
                reason = "ESTACION_NO_DISPONIBLE"
            if reason:
                result = dict(message, reason=reason, timestamp=time.time(), updated_at=time.time())
                self.store_request(result, "DENIED")
                self.reply(message, "DENIED", reason=reason)
                self.log(f"[DENEGADA] {message['operator']} -> {ws_id}: {reason}", "WARNING")
                return
            run = dict(message, started=False, volume=0.0, elapsed=0.0,
                       last_event=time.monotonic(), start_time=datetime.now().isoformat())
            self.active[ws_id] = run
            ws.update(accumulated_volume=0, current_operator=message["operator"],
                      last_request_id=run["id"])
            self.save(ws)
            self.reply(run, "PENDING", reason="ESPERANDO_CONFIRMACION_ENGINE")
<<<<<<< HEAD
            self.emit("ws_orders", dict(run, type="START"))
            self.log(f"[SOLICITUD] {message['operator']} -> {ws_id}, {duration} s, ID {run['id']}")
=======
            self.emit("ws_orders", dict(run, type="START", expires=time.time()+12))
            print(f"[SOLICITUD] {message}", flush=True)
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

    def handle_event(self, message):
        with self.lock, connection():
            ws_id, request_id = message.get("ws"), message.get("id")
            if not field(ws_id) or ws_id not in self.stations or not field(request_id):
                return
            previous = self.load_request(request_id)
            if previous and previous[0] in ("FINAL", "DENIED"):
                return  # Evento duplicado de una transacción ya cerrada.
            active = self.active.get(ws_id)
            run = active if active and active["id"] == request_id else None
            kind = message.get("type")
            if run is None:
                if kind != "END":
                    return
                if previous:
                    if previous[1].get("ws") != ws_id:
                        return
                    run = previous[1]
                else:
                    # Compatibilidad con un checkpoint del Engine anterior a la migración.
                    if not field(message.get("operator")):
                        return
                    run = dict(message, volume=0.0, elapsed=0.0,
                               start_time=message.get("start_time", datetime.now().isoformat()))
            ws = self.stations[ws_id]
            if kind == "STARTED":
                run.update(started=True, last_event=time.monotonic())
                self.save(ws)
                self.reply(run, "ACK", reason="RIEGO_INICIADO")
            elif kind == "REJECTED":
                self.finish(ws_id, message.get("reason", "RECHAZADO_ENGINE"))
            elif kind in ("TELEMETRY", "END"):
                volume, elapsed = float(message["volume"]), float(message["elapsed"])
                flow = float(message.get("flow", 0))
                if not (run["volume"] <= volume <= 1000000 and
                        run["elapsed"] <= elapsed <= 7200 and 0 <= flow <= 10000):
                    return
                run.update(volume=volume, elapsed=elapsed, last_event=time.monotonic())
                if kind == "END":
                    if active and active["id"] == request_id:
                        self.active.pop(ws_id)
                    if message.get("reason") == "FUGA" and ws.get("last_request_id") == request_id:
                        ws["health"] = "KO"
                    self.finalize(run, message.get("reason", "COMPLETADO"))
                else:
                    ws.update(accumulated_volume=volume, current_flow=flow)
                    self.save(ws)
                    self.reply(run, "UPDATE", volume=volume, elapsed=elapsed, flow=flow)

    def admin(self, action, target, duration=10):
        with self.lock, connection():
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
                        "submitted_at": time.time(),
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

            self.log(f"[PANEL] Acción {action} sobre {target}")
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

                with self.lock, connection():
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

                        with connection() as conn:
                            row = conn.execute(
                                "SELECT * FROM watering_stations WHERE id=?",
                                (candidate,),
                            ).fetchone()

                        if not row:
                            send_line(sock, "STATUS#ERROR#STATION_NOT_REGISTERED")
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
                        self.log(f"[MONITOR] {ws_id} conectado y registrado")

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
            self.log(f"[SOCKET] {error}", "ERROR")

        finally:
            with self.lock, connection():
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

            self.log(f"[SOCKET] Escuchando en {port}")

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

            with self.lock, connection():
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
                    if self.recovering:
                        continue
                    limit = 6 if run["started"] else 12

                    if time.monotonic() - run["last_event"] > limit:
                        self.stop_order(
                            ws_id, "SIN_RESPUESTA_ENGINE"
                        )
                        self.finish(
                            ws_id, "SIN_RESPUESTA_ULTIMA_MEDIDA"
                        )

    def offsets(self):
        with connection() as conn:
            return {(r["topic"], r["partition_id"]): r["next_offset"]
                    for r in conn.execute("SELECT * FROM kafka_offsets")}

    def flush_outbox(self, producer):
        with connection() as conn:
            rows = conn.execute("SELECT * FROM central_outbox ORDER BY id LIMIT 50").fetchall()
        for row in rows:
            publish(producer, row["topic"], json.loads(row["payload"]))
            # La caída entre ACK y DELETE implica un duplicado, nunca una pérdida.
            with connection() as conn:
                conn.execute("DELETE FROM central_outbox WHERE id=?", (row["id"],))

    def process_record(self, record):
        """Confirmar el efecto y el offset en la MISMA transacción SQLite."""
        with self.lock, connection() as conn:
            try:
                message = json.loads(record.value.decode("utf-8"))
                if not isinstance(message, dict):
                    raise ValueError("Se esperaba un objeto JSON")
                if message.get("type") in ("REQUEST", "LIST"):
                    self.handle_request(message)
                else:
                    self.handle_event(message)
            except (UnicodeError, ValueError, TypeError, KeyError) as error:
<<<<<<< HEAD
                self.log(f"[MENSAJE INVALIDO] {error}", "WARNING")
=======
                print(f"[MENSAJE INVALIDO] {error}", flush=True)
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
            conn.execute("""
                INSERT INTO kafka_offsets VALUES (?,?,?)
                ON CONFLICT(topic,partition_id) DO UPDATE SET next_offset=excluded.next_offset
            """, (record.topic, record.partition, record.offset+1))

    def kafka(self, broker):
        while True:
            producer, consumer = open_bus(broker, ("fo_requests", "ws_events"), self.offsets())
            targets = consumer.end_offsets(consumer.assignment())
            with connection() as conn:
                for partition in consumer.assignment():
                    conn.execute("INSERT OR IGNORE INTO kafka_offsets VALUES (?,?,?)",
                                 (partition.topic, partition.partition, consumer.position(partition)))
            self.recovering = True
            try:
                while True:
                    for batch in consumer.poll(timeout_ms=200).values():
                        for record in batch:
                            self.process_record(record)
                    confirmed = self.offsets()
                    if all(confirmed.get((p.topic, p.partition), 0) >= target
                           for p, target in targets.items()):
                        self.recovering = False
<<<<<<< HEAD
                        if not self.ready.is_set():
                            self.log("[KAFKA] Conectado. Recuperación de mensajes completada.")
                        self.ready.set()
                    self.flush_outbox(producer)
            except Exception as error:
                self.log(f"[KAFKA] Reconectando: {error}", "ERROR")
=======
                        self.ready.set()
                    self.flush_outbox(producer)
            except Exception as error:
                print(f"[KAFKA] Reconectando: {error}", flush=True)
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
                # Si falló una escritura, descartar también su caché en memoria.
                with self.lock:
                    self.restore_requests()
            finally:
                self.ready.clear()
                consumer.close()
                producer.close()


def create_app(central):
<<<<<<< HEAD
    """Separar HTTP del arranque permite probar las altas sin Docker ni Kafka."""
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 256 * 1024
=======
    """Construye la API sin arrancar Kafka, sockets ni el servidor HTTP."""
    app = Flask(__name__)
    # El catálogo tiene un máximo por lista y también un límite de cuerpo HTTP.
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
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

<<<<<<< HEAD
    @app.get("/api/logs")
    def logs():
        try:
            after = int(request.args.get("after", "0"))
            if after < 0:
                raise ValueError
        except ValueError:
            return jsonify({"error": "El cursor de logs debe ser un entero no negativo"}), 400
        response = jsonify(central.logs.read(after, request.args.get("session")))
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.post("/api/catalog")
    def catalog():
        try:
            return jsonify(central.register_catalog(request.get_json(silent=True)))
        except ValueError as error:
            return jsonify({"error": str(error)}), 400
=======
    @app.post("/api/admin/catalog")
    def register_catalog():
        # El lanzador puede ampliar el catálogo sin una clave adicional.
        # Se mantienen la validación del lote y la actualización atómica.
        try:
            result = central.register_catalog(request.get_json(silent=True))
        except ValueError as error:
            return jsonify(error=str(error)), 400
        return jsonify(result)
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65

    @app.post("/api/stations/<ws_id>/<action>")
    def action(ws_id, action):
        body = request.get_json(silent=True) or {}

        result, code = central.admin(
            action, ws_id, body.get("duration", 10)
        )

        return jsonify(result), code

    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket-port", type=int, required=True)
    parser.add_argument("--kafka-broker", required=True)
    args = parser.parse_args()
<<<<<<< HEAD
    init_db()
    central = Central()
=======

    # Un escenario de una sola estación no debe añadir las otras dos del demo.
    # La migración sigue conservando todas las identidades y datos ya guardados.
    seed_demo = os.environ.get("WM_SEED_DEMO", "0").strip().lower() in ("1", "true", "yes")
    init_db(seed_demo=seed_demo)
    central = Central()

>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
    for target, values in (
        (central.kafka, (args.kafka_broker,)),
        (central.sockets, (args.socket_port,)),
        (central.watchdog, ()),
    ):
<<<<<<< HEAD
        threading.Thread(target=target, args=values, daemon=True).start()
    create_app(central).run(
=======
        threading.Thread(
            target=target, args=values, daemon=True
        ).start()

    app = create_app(central)
    app.run(
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
