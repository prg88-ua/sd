#primera version para ver como va, falta que la conexion sea persistente y que se vigile al Engine, 
# pero de momento sirve para probar el registro de la estación en Central.
import argparse
import socket


FORMAT = "utf-8"

# Plazo de esta implementación para conectar y esperar la respuesta.
# No es un tiempo impuesto por el enunciado.
RESPONSE_TIMEOUT = 5.0

# Mismo límite de línea que utiliza el WM_Central propuesto.
MAX_MESSAGE_BYTES = 4096


def send_message(conn, message):
    #Envía el mensaje como UTF-8 y añade el salto de línea del contrato.
    # sendall envía todos los bytes. No enviamos la cabecera de 64 bytes
    # que utilizaba el cliente de la Práctica 1.
    conn.sendall((message + "\n").encode(FORMAT))


def receive_message(conn):
    #Recibe una respuesta completa, aunque llegue en varios fragmentos.
    buffer = b""

    while True:
        received = conn.recv(1024)

        # Un resultado vacío significa que Central ha cerrado la conexión.
        if not received:
            raise ConnectionError(
                "Central cerró la conexión antes de completar la respuesta."
            )

        buffer += received

        if b"\n" in buffer:
            # Esta versión espera una sola respuesta y después cierra.
            # Al hacer el Monitor persistente habrá que conservar el sobrante.
            line, _ = buffer.split(b"\n", 1)

            if len(line) > MAX_MESSAGE_BYTES:
                raise ValueError("La respuesta de Central es demasiado larga.")

            # Decodificamos después de completar la línea: una tilde UTF-8
            # podría haber llegado repartida entre dos llamadas a recv.
            return line.decode(FORMAT)

        if len(buffer) > MAX_MESSAGE_BYTES:
            raise ValueError(
                "Central envió demasiados bytes sin terminar la línea."
            )


def parse_status(message):
    #Comprueba que Central ha respondido con un STATUS válido.
    if "\r" in message or "\x00" in message:
        raise ValueError("La respuesta contiene caracteres no permitidos.")

    fields = message.split("#")

    # Todas las respuestas del contrato tienen tres campos:
    # STATUS, OK/ERROR y REGISTERED/motivo.
    if len(fields) != 3 or fields[0] != "STATUS":
        raise ValueError(f"Respuesta fuera del contrato: {message!r}")

    result, detail = fields[1], fields[2]

    if result == "OK" and detail == "REGISTERED":
        return result, detail

    if result == "ERROR" and detail.strip():
        return result, detail

    raise ValueError(f"Respuesta fuera del contrato: {message!r}")


def main(argv=None):
    # Los argumentos permiten usar el mismo programa para varias estaciones
    # sin cambiar el código. argv también facilita las pruebas.
    parser = argparse.ArgumentParser(
        description="WM_WS_M: registro inicial de una estación en Central."
    )
    parser.add_argument("--central-host", required=True)
    parser.add_argument("--central-port", type=int, required=True)
    parser.add_argument("--ws-id", required=True)
    parser.add_argument("--location", required=True)
    args = parser.parse_args(argv)

    if not args.central_host.strip():
        parser.error("--central-host no puede estar vacío.")

    if not 1 <= args.central_port <= 65535:
        parser.error("--central-port debe estar entre 1 y 65535.")

    # Validamos antes de construir la trama para que un campo no introduzca
    # separadores adicionales o un segundo mensaje.
    for name, value in (
        ("--ws-id", args.ws_id),
        ("--location", args.location),
    ):
        if not value.strip():
            parser.error(f"{name} no puede estar vacío.")

        if any(
            character in value
            for character in ("#", "\n", "\r", "\x00")
        ):
            parser.error(f"{name} contiene un carácter no permitido.")

    request = f"REGISTRO#{args.ws_id}#{args.location}"

    if len(request.encode(FORMAT)) > MAX_MESSAGE_BYTES:
        parser.error("El mensaje de registro es demasiado largo.")

    try:
        # create_connection crea el socket TCP y conecta con host y puerto.
        # El timeout también se aplica a las lecturas y escrituras.
        # with garantiza que se cierra al salir, incluso si ocurre un error.
        with socket.create_connection(
            (args.central_host, args.central_port),
            timeout=RESPONSE_TIMEOUT,
        ) as conn:
            print(
                f"[CONEXION] Central: "
                f"{args.central_host}:{args.central_port}"
            )

            send_message(conn, request)
            print(f"[ENVIADO] {request}")

            response = receive_message(conn)
            print(f"[RECIBIDO] {response}")

            result, detail = parse_status(response)

            if result == "ERROR":
                explanations = {
                    "INVALID_MESSAGE": (
                        "Central rechazó el formato del registro."
                    ),
                    "DUPLICATE_STATION": (
                        "Ya existe otro Monitor conectado con este ID."
                    ),
                }

                # Si Central añade otro motivo, también lo mostramos.
                explanation = explanations.get(
                    detail,
                    f"Central rechazó el registro: {detail}",
                )
                print(f"[REGISTRO RECHAZADO] {explanation}")
                return 1

            print(
                f"[REGISTRO CORRECTO] "
                f"{args.ws_id}: {args.location}"
            )

        print("[CONEXION CERRADA] Prueba de registro terminada.")
        return 0

    except socket.timeout:
        print(
            "[ERROR] Se agotó el tiempo de conexión "
            "o espera de respuesta."
        )
        return 1

    except UnicodeDecodeError:
        print("[ERROR] La respuesta de Central no es UTF-8 válido.")
        return 1

    except ValueError as error:
        print(f"[ERROR DE PROTOCOLO] {error}")
        return 1

    except OSError as error:
        # Incluye conexión rechazada, host inaccesible o conexión interrumpida.
        print(f"[ERROR DE CONEXION] {error}")
        return 1


# Importar el archivo no ejecuta el Monitor.
# Al ejecutarlo directamente, 0 significa éxito y 1 significa fallo.
if __name__ == "__main__":
    raise SystemExit(main())