"""Operario: lista estaciones, recupera ordenes previas y muestra mediciones."""
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

                # Cada mensaje renueva el plazo de espera.
                deadline = time.monotonic() + timeout

    raise TimeoutError(
        "CENTRAL no responde; comprueba sus logs y la conexion"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-broker", required=True)
    parser.add_argument("--id-operario", required=True)
    parser.add_argument("--ws")
    parser.add_argument("--duracion", type=int, default=10)
    parser.add_argument("--archivo")
    parser.add_argument("--listar", action="store_true")
    args = parser.parse_args()

    tasks = []

    if args.archivo:
        with open(args.archivo, encoding="utf-8-sig") as stream:
            for number, line in enumerate(stream, 1):
                if not line.strip():
                    continue

                try:
                    ws, duration = line.strip().split(",")
                    tasks.append((ws.strip(), int(duration)))

                except ValueError:
                    parser.error(
                        f"Linea {number}: usa WS-01,10"
                    )

    elif args.ws:
        tasks.append((args.ws, args.duracion))

    if tasks and any(
        not ws or not 1 <= duration <= 3600
        for ws, duration in tasks
    ):
        parser.error(
            "La duracion debe estar entre 1 y 3600 segundos"
        )

    producer, consumer = open_bus(
        args.kafka_broker, ("fo_responses",)
    )

    try:
        listing_id = uuid.uuid4().hex

        publish(producer, "fo_requests", {
            "type": "LIST",
            "id": listing_id,
            "operator": args.id_operario,
        })

        stations = []
        active_order = None
        last_completed = None

        for message in wait_response(
            consumer, listing_id, 20
        ):
            if message["type"] == "DENIED":
                reason = message.get("reason", "DESCONOCIDO")
                if reason == "OPERARIO_NO_REGISTRADO":
                    raise ValueError(
                        f"Excepción: El operario '{args.id_operario}' no existe en el sistema."
                    )
                raise ValueError(
                    f"Excepción: Petición de operario denegada por CENTRAL ({reason})"
                )

            if message["type"] == "STATIONS":
                stations = message.get("stations", [])
                active_order = message.get("active_order")
                last_completed = message.get("last_completed")

                print(f"--- Estaciones disponibles para operario {args.id_operario} ---")
                for ws in stations:
                    print(
                        f"{ws['id']} | {ws['location']} "
                        f"| {ws['status']}"
                    )

                break

        # Validar existencia de las estaciones solicitadas
        valid_station_ids = {ws["id"] for ws in stations}
        for ws, _ in tasks:
            if ws not in valid_station_ids:
                raise ValueError(
                    f"Excepción: La estación de riego '{ws}' no existe en el sistema."
                )

        # Requisito de Resiliencia: si el operario se cerro mientras se prestaba un servicio,
        # al reabrir el operario ve el resultado del servicio que habia pedido antes de caerse.
        saved_state = load_fo_state(args.id_operario)
        recovered_ws = None

        if active_order:
            act_id = active_order["id"]
            act_ws = active_order["ws"]
            recovered_ws = act_ws
            print(
                f"\n[RECUPERACION] Detectada orden previa aún en curso en {act_ws} "
                f"(ID={act_id}). Reanudando seguimiento...",
                flush=True,
            )

            for msg in wait_response(consumer, act_id, 25):
                k = msg["type"]
                if k == "UPDATE":
                    print(
                        f"{msg['elapsed']:.1f}s | "
                        f"{msg['flow']:.1f} L/min | "
                        f"{msg['volume']:.2f} L"
                    )
                elif k == "END":
                    print(
                        f"\n[FIN RIEGO PREVIO] {msg['reason']} | "
                        f"{msg['elapsed']:.1f}s | "
                        f"{msg['volume']:.2f} L\n"
                    )
                    clear_fo_state(args.id_operario)
                    break
                elif k == "DENIED":
                    print(f"\n[DENEGADO RIEGO PREVIO] {msg['reason']}\n")
                    clear_fo_state(args.id_operario)
                    break

        elif last_completed and (
            (saved_state and saved_state.get("id") == last_completed.get("id"))
            or (time.time() - last_completed.get("timestamp", 0) < 300)
        ):
            recovered_ws = last_completed.get("ws")
            print(
                f"\n[RECUPERACION] Resultado del servicio solicitado antes de caerse:\n"
                f"FIN: {last_completed['reason']} | "
                f"{last_completed['elapsed']:.1f}s | "
                f"{last_completed['volume']:.2f} L\n",
                flush=True,
            )
            clear_fo_state(args.id_operario)

        elif saved_state:
            clear_fo_state(args.id_operario)

        # Si solo se arrancó para listar o el riego pedido ya se recuperó
        remaining_tasks = tasks
        if recovered_ws and len(tasks) == 1 and tasks[0][0] == recovered_ws:
            remaining_tasks = []

        for index, (ws, duration) in enumerate(remaining_tasks):
            request_id = uuid.uuid4().hex

            save_fo_state(args.id_operario, {
                "id": request_id,
                "ws": ws,
                "duration": duration,
                "operator": args.id_operario,
                "time": time.time(),
            })

            publish(producer, "fo_requests", {
                "type": "REQUEST",
                "id": request_id,
                "operator": args.id_operario,
                "ws": ws,
                "duration": duration,
            })

            print(f"[SOLICITUD] {ws}, {duration}s")

            for message in wait_response(
                consumer, request_id, 20
            ):
                kind = message["type"]

                if kind == "PENDING":
                    print(
                        "CENTRAL comprueba el arranque con el Engine..."
                    )

                elif kind == "ACK":
                    print(
                        "Riego iniciado y confirmado por el Engine"
                    )

                elif kind == "UPDATE":
                    print(
                        f"{message['elapsed']:.1f}s | "
                        f"{message['flow']:.1f} L/min | "
                        f"{message['volume']:.2f} L"
                    )

                elif kind == "DENIED":
                    clear_fo_state(args.id_operario)
                    reason = message.get("reason", "DESCONOCIDO")
                    if reason == "ESTACION_NO_EXISTE":
                        raise ValueError(
                            f"Excepción: La estación '{ws}' no existe en el sistema."
                        )
                    elif reason == "OPERARIO_NO_REGISTRADO":
                        raise ValueError(
                            f"Excepción: El operario '{args.id_operario}' no existe en el sistema."
                        )
                    else:
                        print(f"DENEGADO: {reason}")
                    break

                elif kind == "END":
                    clear_fo_state(args.id_operario)
                    print(
                        f"FIN: {message['reason']} | "
                        f"{message['elapsed']:.1f}s | "
                        f"{message['volume']:.2f} L"
                    )
                    break

            if index + 1 < len(remaining_tasks):
                print(
                    "Esperando 4 segundos para "
                    "la siguiente peticion..."
                )
                time.sleep(4)

    except (TimeoutError, KeyboardInterrupt) as error:
        print(f"[FO] {error}")

    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
