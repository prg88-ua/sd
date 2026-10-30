"""Operario: menú continuo y recuperación sin duplicar solicitudes."""
import argparse
from collections import defaultdict, deque
import json
from pathlib import Path
<<<<<<< HEAD
=======
import signal
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
import sys
import time
import uuid

# También admite: python WM_FO/WM_FO.py desde la raíz del proyecto.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wm_common import field, messages, open_bus, publish
from wm_state import atomic_json, safe_id, state_dir
from kafka.errors import KafkaError


def get_fo_state_file(operator_id):
    return state_dir() / f"fo_state_{safe_id(operator_id)}.json"


def save_fo_state(operator_id, data):
    # Un error de disco se comunica: no prometer recuperación sin haber guardado.
    atomic_json(get_fo_state_file(operator_id), data)


def load_fo_state(operator_id):
    path = get_fo_state_file(operator_id)
    legacy = Path(f"fo_state_{safe_id(operator_id)}.json")
    if not path.exists() and legacy.exists():
        data = json.loads(legacy.read_text(encoding="utf-8"))
        save_fo_state(operator_id, {"pending": data})
        legacy.unlink()
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {"pending": data} if "id" in data else data


def clear_fo_state(operator_id):
    state = load_fo_state(operator_id)
    state.pop("pending", None)
    save_fo_state(operator_id, state)


class ResponseInbox:
    """Conserva el lote completo aunque el solicitante solo necesite un mensaje."""

    def __init__(self, consumer, operator_id):
        self.consumer = consumer
        self.operator_id = operator_id
        self.pending = defaultdict(deque)
        self.finished = set()

    def wait(self, request_id, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.pending[request_id]:
                # Drenar TODO el poll antes de devolver STATIONS. Así no se
                # abandona un generador que todavía contiene el END recuperado.
                for message in messages(self.consumer):
                    key = message.get("id")
                    if (message.get("operator") == self.operator_id
                            and field(key) and key not in self.finished):
                        self.pending[key].append(message)
            while self.pending[request_id]:
                message = self.pending[request_id].popleft()
                deadline = time.monotonic() + timeout
                yield message
        raise TimeoutError("Imposible conectar con WM_Central; la orden pendiente se conserva")

    def forget(self, request_id):
        self.pending.pop(request_id, None)
        self.finished.add(request_id)

    def close(self):
        self.consumer.close()


def wait_response(consumer, request_id, timeout):
    yield from consumer.wait(request_id, timeout)


def get_stations(producer, consumer, operator_id):
    listing_id = uuid.uuid4().hex
    saved = load_fo_state(operator_id).get("pending")
    publish(producer, "fo_requests", {
        "type": "LIST", "id": listing_id, "operator": operator_id,
        "recover_id": saved["id"] if saved else None,
    })
    try:
        for message in wait_response(consumer, listing_id, 20):
            if message["type"] == "DENIED":
                raise ValueError(f"Operario '{operator_id}': {message['reason']}")
            if message["type"] == "STATIONS":
                return (message.get("stations", []), message.get("active_order"),
                        message.get("last_completed"))
    finally:
        consumer.forget(listing_id)


def show_result(operator_id, result):
    if "volume" in result:
        print(f"\n[FIN DE RIEGO] {result['reason']} | "
              f"{result['elapsed']:.1f}s | {result['volume']:.2f} L")
        if not result.get("final", True):
            print("[AVISO] Última medida disponible; el Engine puede corregirla al recuperarse.")
    else:
        print(f"[DENEGADO] {result['reason']}")
    state = load_fo_state(operator_id)
    pending = state.get("pending")
    if pending is None or pending["id"] == result["id"]:
        state.pop("pending", None)
    state["seen_result"] = [result["id"], result.get("updated_at", 0)]
    save_fo_state(operator_id, state)


def follow_order(producer, consumer, operator_id, request):
    for message in wait_response(consumer, request["id"], 25):
        kind = message["type"]
        if kind in ("PENDING", "ACK"):
            print("[ESTADO] " + ("CENTRAL está comprobando el arranque..."
                                if kind == "PENDING" else "Riego confirmado por el Engine."))
        elif kind == "UPDATE":
            print(f"[RIEGO] {message['elapsed']:.1f}s | "
                  f"{message['flow']:.1f} L/min | {message['volume']:.2f} L")
        elif kind in ("END", "DENIED"):
            show_result(operator_id, message)
            consumer.forget(request["id"])
            return


def execute_irrigation(producer, consumer, operator_id, ws, duration, known_stations):
    if ws not in {s["id"] for s in known_stations}:
        raise ValueError(f"La estación de riego '{ws}' no existe en el sistema.")
    if type(duration) is not int or not 1 <= duration <= 3600:
        raise ValueError("La duración debe ser un entero entre 1 y 3600 segundos.")
    state = load_fo_state(operator_id)
    if state.get("pending"):
        raise ValueError("Hay una orden pendiente: consulta su recuperación antes de pedir otra.")
    request = dict(type="REQUEST", id=uuid.uuid4().hex, operator=operator_id,
                   ws=ws, duration=duration, submitted_at=time.time())
    state["pending"] = request
    save_fo_state(operator_id, state)
    # Si se pierde el ACK, al reiniciar se reenvía ESTE ID, no una orden nueva.
    publish(producer, "fo_requests", request)
    print(f"[SOLICITUD] {ws}, {duration}s")
    follow_order(producer, consumer, operator_id, request)


def check_recovery(producer, consumer, operator_id, active_order, last_completed):
    state = load_fo_state(operator_id)
    saved = state.get("pending")
    if active_order:
        state["pending"] = active_order
        save_fo_state(operator_id, state)
        print(f"[RECUPERACIÓN] Siguiendo el riego {active_order['id']} en {active_order['ws']}")
        follow_order(producer, consumer, operator_id, active_order)
    elif last_completed and (not saved or saved["id"] == last_completed["id"]):
        token = [last_completed["id"], last_completed.get("updated_at", 0)]
        if saved or state.get("seen_result") != token:
            print("[RECUPERACIÓN] Resultado guardado por CENTRAL:")
            show_result(operator_id, last_completed)
    elif saved:
        print("[RECUPERACIÓN] Confirmando la orden pendiente con su mismo identificador.")
        request = dict(saved, type="REQUEST")
<<<<<<< HEAD
        # Conservar la fecha antigua solo como información. Central decide por
        # el mismo ID persistente, sin comparar relojes de equipos distintos.
        request.setdefault("submitted_at", saved.get("time", 0))
        publish(producer, "fo_requests", request)
        follow_order(producer, consumer, operator_id, request)


def execute_file(producer, consumer, operator_id, path, stations):
    """Cada línea es una petición independiente; un error no aborta las demás."""
    with open(path, encoding="utf-8-sig") as stream:
        lines = [(n, line.strip()) for n, line in enumerate(stream, 1) if line.strip()]
    for index, (number, line) in enumerate(lines):
        try:
            ws, text = line.split(",")
            execute_irrigation(producer, consumer, operator_id, ws.strip(), int(text), stations)
        except ValueError as error:
            print(f"[ERROR] Línea {number}: {error}")
        except (TimeoutError, KafkaError, OSError):
            # Se desconoce si el riego terminó: no solapar el siguiente servicio.
            print("[AVISO] Lote pausado; consulta la orden pendiente antes de continuar.")
            raise
        if index + 1 < len(lines):
            print("Esperando 4 segundos para la siguiente petición...")
            time.sleep(4)


def main():
    parser = argparse.ArgumentParser(description="Aplicación del Operario de Campo (WM_FO)")
    parser.add_argument("--kafka-broker", required=True)
    parser.add_argument("--id-operario", required=True)
    parser.add_argument("--file", "--batch-file", dest="batch_file",
                        help="Ejecutar este archivo tras recuperar el servicio pendiente")
    args = parser.parse_args()
    safe_id(args.id_operario)
=======
        # Un checkpoint antiguo sin fecha no debe activar un riego histórico.
        request.setdefault("submitted_at", saved.get("time", 0))
        publish(producer, "fo_requests", request)
        follow_order(producer, consumer, operator_id, request)


def execute_file(producer, consumer, operator_id, path, stations):
    """Cada línea es una petición independiente; un error no aborta las demás."""
    with open(path, encoding="utf-8-sig") as stream:
        lines = [(n, line.strip()) for n, line in enumerate(stream, 1) if line.strip()]
    for index, (number, line) in enumerate(lines):
        try:
            ws, text = line.split(",")
            execute_irrigation(producer, consumer, operator_id, ws.strip(), int(text), stations)
        except ValueError as error:
            print(f"[ERROR] Línea {number}: {error}")
        except (TimeoutError, KafkaError, OSError):
            # Se desconoce si el riego terminó: no solapar el siguiente servicio.
            print("[AVISO] Lote pausado; consulta la orden pendiente antes de continuar.")
            raise
        if index + 1 < len(lines):
            print("Esperando 4 segundos para la siguiente petición...")
            time.sleep(4)


def termination_signal(signum, frame):
    # Docker envía SIGTERM: reutilizar el mismo cierre que al pulsar Ctrl+C.
    raise KeyboardInterrupt


def run(args):
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
    producer, raw_consumer = open_bus(args.kafka_broker, ("fo_responses",))
    consumer = ResponseInbox(raw_consumer, args.id_operario)
    try:
        try:
            stations, active, completed = get_stations(producer, consumer, args.id_operario)
            check_recovery(producer, consumer, args.id_operario, active, completed)
            if args.batch_file:
<<<<<<< HEAD
                # Reutilizamos las validaciones y la pausa de 4 s del menú.
                # Si la recuperación falla, no se empieza un lote nuevo.
=======
                # Recuperar primero impide que el lote solape una orden pendiente.
                # Se conserva la lógica del menú: validación, espera y persistencia.
>>>>>>> ce6e17a39e6545c2362bef76892aa9fe80007e65
                execute_file(producer, consumer, args.id_operario, args.batch_file, stations)
        except (TimeoutError, OSError, KafkaError) as error:
            print(f"[FO] {error}")
        except ValueError as error:
            print(f"[FO] {error}")
            return
        while True:
            print("\n" + "=" * 45)
            print(f"      PANEL OPERARIO: {args.id_operario}")
            print("  1. Listar estaciones y recuperar servicio")
            print("  2. Solicitar un riego (manual)")
            print("  3. Ejecutar riegos desde archivo")
            print("  4. Salir\n" + "=" * 45)
            option = input("Seleccione una opción [1-4]: ").strip()
            if option == "4":
                break
            if option not in ("1", "2", "3"):
                print("[AVISO] Opción inválida.")
                continue
            try:
                stations, active, completed = get_stations(producer, consumer, args.id_operario)
                check_recovery(producer, consumer, args.id_operario, active, completed)
                if option == "1":
                    for station in stations:
                        print(f"{station['id']} | {station['location']} | {station['status']}")
                elif option == "2":
                    available = [s["id"] for s in stations if s["status"] == "AVAILABLE"]
                    print("Estaciones disponibles: " + (", ".join(available) or "Ninguna"))
                    ws = input("ID de estación: ").strip()
                    duration = int(input("Duración en segundos [10]: ").strip() or "10")
                    execute_irrigation(producer, consumer, args.id_operario, ws, duration, stations)
                else:
                    path = input("Ruta del archivo [peticiones.txt]: ").strip() or "peticiones.txt"
                    execute_file(producer, consumer, args.id_operario, path, stations)
            except (ValueError, TimeoutError, OSError, KafkaError) as error:
                print(f"[FO] {error}")
    except (EOFError, KeyboardInterrupt):
        print("\n[FO] Sesión cerrada; el riego en curso no se cancela.")
    finally:
        # Una segunda petición de cierre no debe interrumpir la liberación de clientes.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        consumer.close()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="Aplicación del Operario de Campo (WM_FO)")
    parser.add_argument("--kafka-broker", required=True)
    parser.add_argument("--id-operario", required=True)
    parser.add_argument("--file", "--batch-file", dest="batch_file",
                        help="Ejecutar este archivo después de recuperar el estado y después mostrar el menú")
    args = parser.parse_args()
    safe_id(args.id_operario)
    previous = signal.signal(signal.SIGTERM, termination_signal)
    try:
        run(args)
    except KeyboardInterrupt:
        # También se puede cerrar mientras se espera la conexión inicial a Kafka.
        print("\n[FO] Sesión cerrada; se conserva el estado pendiente.")
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    main()
