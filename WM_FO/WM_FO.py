"""Operario (WM_FO): interfaz interactiva por menú, persistencia y seguimiento de riegos."""
import argparse
import json
import os
import time
import uuid

from wm_common import messages, open_bus, publish


def get_fo_state_file(operator_id):
    return f"fo_state_{operator_id}.json"


def save_fo_state(operator_id, data):
    state_file = get_fo_state_file(operator_id)
    tmp_file = f"{state_file}.tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_file, state_file)
    except Exception:
        pass


def load_fo_state(operator_id):
    state_file = get_fo_state_file(operator_id)
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def clear_fo_state(operator_id):
    state_file = get_fo_state_file(operator_id)
    try:
        if os.path.exists(state_file):
            os.remove(state_file)
    except OSError:
        pass


def wait_response(consumer, request_id, timeout):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        for message in messages(consumer):
            if message.get("id") == request_id:
                yield message
                # Cada mensaje renueva el plazo de espera
                deadline = time.monotonic() + timeout

    raise TimeoutError("CENTRAL no responde; comprueba sus logs y la conexión")


def get_stations(producer, consumer, operator_id):
    """Consulta la lista de estaciones disponibles y comprueba la validez del operario."""
    listing_id = uuid.uuid4().hex
    publish(producer, "fo_requests", {
        "type": "LIST",
        "id": listing_id,
        "operator": operator_id,
    })

    for message in wait_response(consumer, listing_id, 20):
        if message["type"] == "DENIED":
            reason = message.get("reason", "DESCONOCIDO")
            if reason == "OPERARIO_NO_REGISTRADO":
                raise ValueError(
                    f"Excepción: El operario '{operator_id}' no existe en el sistema."
                )
            raise ValueError(
                f"Excepción: Petición de operario denegada por CENTRAL ({reason})"
            )

        if message["type"] == "STATIONS":
            return (
                message.get("stations", []),
                message.get("active_order"),
                message.get("last_completed"),
            )

    return [], None, None


def execute_irrigation(producer, consumer, operator_id, ws, duration, known_stations):
    """Ejecuta una solicitud de riego individual y muestra la telemetría en tiempo real."""
    valid_ids = {s["id"] for s in known_stations}
    if ws not in valid_ids:
        raise ValueError(f"Excepción: La estación de riego '{ws}' no existe en el sistema.")

    if not (1 <= duration <= 3600):
        raise ValueError("La duración debe estar entre 1 y 3600 segundos.")

    request_id = uuid.uuid4().hex

    save_fo_state(operator_id, {
        "id": request_id,
        "ws": ws,
        "duration": duration,
        "operator": operator_id,
        "time": time.time(),
    })

    publish(producer, "fo_requests", {
        "type": "REQUEST",
        "id": request_id,
        "operator": operator_id,
        "ws": ws,
        "duration": duration,
    })

    print(f"\n[SOLICITUD ENVIADA] Estación: {ws} | Duración: {duration}s")

    for message in wait_response(consumer, request_id, 25):
        kind = message["type"]

        if kind == "PENDING":
            print("[ESTADO] CENTRAL comprobando el arranque con el Engine...")

        elif kind == "ACK":
            print("[ESTADO] Riego iniciado y confirmado por el Engine.")

        elif kind == "UPDATE":
            print(
                f"[RIEGO EN CURSO] Tiempo: {message['elapsed']:.1f}s | "
                f"Caudal: {message['flow']:.1f} L/min | "
                f"Volumen: {message['volume']:.2f} L"
            )

        elif kind == "DENIED":
            clear_fo_state(operator_id)
            reason = message.get("reason", "DESCONOCIDO")
            if reason == "ESTACION_NO_EXISTE":
                raise ValueError(f"Excepción: La estación '{ws}' no existe en el sistema.")
            elif reason == "OPERARIO_NO_REGISTRADO":
                raise ValueError(f"Excepción: El operario '{operator_id}' no existe en el sistema.")
            else:
                print(f"[DENEGADO] {reason}")
            break

        elif kind == "END":
            clear_fo_state(operator_id)
            print(
                f"\n[FIN DE RIEGO] Motivo: {message['reason']} | "
                f"Tiempo total: {message['elapsed']:.1f}s | "
                f"Volumen total: {message['volume']:.2f} L\n"
            )
            break


def check_recovery(producer, consumer, operator_id, active_order, last_completed):
    """Comprueba si el operario se cerró durante un riego previo y muestra el resultado."""
    saved_state = load_fo_state(operator_id)

    if active_order:
        act_id = active_order["id"]
        act_ws = active_order["ws"]
        print(
            f"\n>>> [RECUPERACIÓN] Detectado riego previo aún en curso en {act_ws} "
            f"(ID={act_id}). Reanudando seguimiento...",
            flush=True,
        )

        for msg in wait_response(consumer, act_id, 25):
            k = msg["type"]
            if k == "UPDATE":
                print(
                    f"[RIEGO] {msg['elapsed']:.1f}s | "
                    f"{msg['flow']:.1f} L/min | "
                    f"{msg['volume']:.2f} L"
                )
            elif k == "END":
                print(
                    f"\n>>> [FIN RIEGO PREVIO] {msg['reason']} | "
                    f"Tiempo: {msg['elapsed']:.1f}s | "
                    f"Volumen: {msg['volume']:.2f} L\n"
                )
                clear_fo_state(operator_id)
                break
            elif k == "DENIED":
                print(f"\n>>> [DENEGADO RIEGO PREVIO] {msg['reason']}\n")
                clear_fo_state(operator_id)
                break

    elif last_completed and (
        (saved_state and saved_state.get("id") == last_completed.get("id"))
        or (time.time() - last_completed.get("timestamp", 0) < 300)
    ):
        print(
            f"\n>>> [RECUPERACIÓN] El servicio solicitado antes de cerrarse ya finalizó:\n"
            f"    Estación: {last_completed.get('ws')} | "
            f"Motivo: {last_completed['reason']} | "
            f"Tiempo: {last_completed['elapsed']:.1f}s | "
            f"Volumen: {last_completed['volume']:.2f} L\n",
            flush=True,
        )
        clear_fo_state(operator_id)

    elif saved_state:
        clear_fo_state(operator_id)


def main():
    parser = argparse.ArgumentParser(description="Aplicación del Operario de Campo (WM_FO)")
    parser.add_argument("--kafka-broker", required=True, help="Host y puerto de Kafka (ej. localhost:9092)")
    parser.add_argument("--id-operario", required=True, help="Identificador del operario (ej. FO-01)")
    args = parser.parse_args()

    producer, consumer = open_bus(args.kafka_broker, ("fo_responses",))

    try:
        # 1. Autenticar operario y verificar estado previo
        stations, active_order, last_completed = get_stations(producer, consumer, args.id_operario)
        check_recovery(producer, consumer, args.id_operario, active_order, last_completed)

        # 2. Bucle principal del Menú Interactivo
        while True:
            print("\n" + "=" * 45)
            print(f"      PANEL OPERARIO: {args.id_operario}")
            print("=" * 45)
            print("  1. Listar estaciones de riego y estado")
            print("  2. Solicitar un riego (manual)")
            print("  3. Ejecutar riegos desde archivo")
            print("  4. Salir")
            print("=" * 45)

            try:
                opcion = input("Seleccione una opción [1-4]: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo del panel...")
                break

            if opcion == "1":
                stations, _, _ = get_stations(producer, consumer, args.id_operario)
                print("\n--- ESTACIONES REGISTRADAS EN CENTRAL ---")
                for s in stations:
                    print(f"  • {s['id']} | {s['location']} | Estado: {s['status']}")
                print("-" * 42)

            elif opcion == "2":
                stations, _, _ = get_stations(producer, consumer, args.id_operario)
                valid_ids = [s["id"] for s in stations]
                print(f"Estaciones disponibles: {', '.join(valid_ids) if valid_ids else 'Ninguna'}")

                ws_id = input("Introduce ID de la estación (ej. WS-01): ").strip()
                dur_str = input("Introduce la duración en segundos [10]: ").strip() or "10"

                try:
                    duration = int(dur_str)
                    execute_irrigation(producer, consumer, args.id_operario, ws_id, duration, stations)
                except ValueError as err:
                    # Lanza la excepción requerida
                    raise err

            elif opcion == "3":
                stations, _, _ = get_stations(producer, consumer, args.id_operario)
                archivo = input("Ruta del archivo [peticiones.txt]: ").strip() or "peticiones.txt"

                if not os.path.exists(archivo):
                    print(f"[ERROR] No se encuentra el archivo '{archivo}'")
                    continue

                tasks = []
                with open(archivo, encoding="utf-8-sig") as stream:
                    for num, line in enumerate(stream, 1):
                        if not line.strip():
                            continue
                        try:
                            ws, duration = line.strip().split(",")
                            tasks.append((ws.strip(), int(duration.strip())))
                        except ValueError:
                            print(f"[ERROR] Línea {num} no válida en '{archivo}' (formato esperado: WS-01,10)")

                for i, (ws, duration) in enumerate(tasks):
                    execute_irrigation(producer, consumer, args.id_operario, ws, duration, stations)
                    if i + 1 < len(tasks):
                        print("Esperando 4 segundos para la siguiente petición...")
                        time.sleep(4)

            elif opcion == "4":
                print("Cerrando sesión del operario. ¡Hasta luego!")
                break

            else:
                print("[AVISO] Opción inválida. Introduce un número del 1 al 4.")

    except (TimeoutError, KeyboardInterrupt) as error:
        print(f"\n[FO] {error}")

    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()