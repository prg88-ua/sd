"""Operario: lista estaciones y muestra las mediciones de sus riegos."""
import argparse
import time
import uuid

from wm_common import messages, open_bus, publish


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

    if not args.listar and not args.ws and not args.archivo:
        parser.error("Indica --listar, --ws o --archivo")

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

    if any(
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

        for message in wait_response(
            consumer, listing_id, 20
        ):
            if message["type"] == "DENIED":
                print(message["reason"])
                return

            if message["type"] == "STATIONS":
                for ws in message["stations"]:
                    print(
                        f"{ws['id']} | {ws['location']} "
                        f"| {ws['status']}"
                    )

                break

        for index, (ws, duration) in enumerate(tasks):
            request_id = uuid.uuid4().hex

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
                    print(
                        f"DENEGADO: {message['reason']}"
                    )
                    break

                elif kind == "END":
                    print(
                        f"FIN: {message['reason']} | "
                        f"{message['elapsed']:.1f}s | "
                        f"{message['volume']:.2f} L"
                    )
                    break

            if index + 1 < len(tasks):
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