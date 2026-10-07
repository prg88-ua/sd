
#primera version no incluye aun Kafka ni órdenes de riego, solo identificación y comprobación de salud por sockets.
import argparse
import socket
import threading

FORMAT = "utf-8"
CONNECTION_TIMEOUT = 5
MAX_MESSAGE_BYTES = 4096


def send_message(connection, message):
    # El salto de línea delimita el mensaje, como exige nuestro contrato.
    connection.sendall((message + "\n").encode(FORMAT))


class LineReader:
    #Conserva los bytes recibidos hasta tener un mensaje completo.

    def __init__(self, connection):
        self.connection = connection
        self.buffer = b""

    def read_message(self):
        while True:
            # TCP puede entregar varios mensajes juntos o uno en varios trozos.
            # Solo devolvemos la primera línea; guardamos el resto para después.
            if b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                if len(line) > MAX_MESSAGE_BYTES:
                    raise ValueError("Mensaje demasiado largo.")

                message = line.decode(FORMAT)
                if "\r" in message or "\x00" in message:
                    raise ValueError("Mensaje con caracteres no permitidos.")
                return message

            if len(self.buffer) > MAX_MESSAGE_BYTES:
                raise ValueError("Mensaje demasiado largo.")

            # Si hay un timeout, el contenido de self.buffer se conserva.
            # Así no perdemos un mensaje que haya llegado parcialmente.
            chunk = self.connection.recv(1024)
            if not chunk:
                raise ConnectionError("Monitor ha cerrado la conexión.")
            self.buffer += chunk


def parse_identity(message):
    # Engine no elige su ID: lo obtiene del Monitor al que se conecta.
    if message == "STATUS#ERROR#NOT_REGISTERED":
        raise ValueError("Monitor todavía no está registrado en Central.")

    fields = message.split("#")
    if len(fields) != 2 or fields[0] != "IDENTITY" or not fields[1].strip():
        raise ValueError(f"Identificación no válida: {message!r}")

    return fields[1]


def parse_ping(message):
    fields = message.split("#")

    # El contrato usa un número de comprobación. Conservamos su texto original
    # para devolver exactamente el mismo número en la respuesta.
    if (
        len(fields) != 2
        or fields[0] != "PING"
        or not fields[1].isascii()
        or not fields[1].isdecimal()
    ):
        raise ValueError(f"PING no válido: {message!r}")

    return fields[1]


def keyboard_menu(fault_event, stop_event):
    print("\nOpciones: k = avería | r = reparar | s = salir")
    print("Escribe la letra y pulsa Intro.\n")

    while not stop_event.is_set():
        try:
            option = input().strip().lower()
        except EOFError:
            # Sin entrada interactiva, mantenemos activo el servicio de sockets.
            print("[Engine] Entrada de teclado cerrada; Engine sigue funcionando.")
            return

        if stop_event.is_set():
            return

        if option == "k":
            # Event es una bandera segura para compartir entre los dos hilos.
            fault_event.set()
            print("[Engine] Avería activada. Los próximos PONG serán KO.")
        elif option == "r":
            fault_event.clear()
            print("[Engine] Avería reparada. Los próximos PONG serán OK.")
        elif option == "s":
            stop_event.set()
            print("[Engine] Cerrando...")
        else:
            print("[Engine] Opción no válida. Utiliza k, r o s.")


def main():
    parser = argparse.ArgumentParser(description="Engine de Water Management.")
    parser.add_argument("--monitor-host", required=True, help="IP o nombre de Monitor.")
    parser.add_argument("--monitor-port", required=True, type=int, help="Puerto de Monitor.")
    args = parser.parse_args()

    if not 1 <= args.monitor_port <= 65535:
        parser.error("--monitor-port debe estar entre 1 y 65535.")

    # Inicialmente no hay avería y no se ha solicitado salir.
    fault_event = threading.Event()
    stop_event = threading.Event()

    try:
        # Engine es cliente; Monitor debe tener un servidor escuchando.
        # El bloque with cierra el socket tanto al salir como si hay un error.
        with socket.create_connection(
            (args.monitor_host, args.monitor_port),
            timeout=CONNECTION_TIMEOUT,
        ) as connection:
            reader = LineReader(connection)

            # Primero nos identificamos. No respondemos PING antes de obtener ID.
            send_message(connection, "IDENTIFY")
            station_id = parse_identity(reader.read_message())
            print(f"[Engine] Conectado a Monitor. Estación: {station_id}")

            # Un hilo atiende el teclado; el principal atiende los mensajes.
            # Así input() no bloquea las respuestas a las comprobaciones.
            threading.Thread(
                target=keyboard_menu,
                args=(fault_event, stop_event),
                daemon=True,
            ).start()

            # Este timeout corto solo permite revisar si se ha pedido salir.
            # No significa que Monitor haya fallado ni genera un KO por sí solo.
            connection.settimeout(0.5)

            while not stop_event.is_set():
                try:
                    message = reader.read_message()
                except socket.timeout:
                    continue

                try:
                    number = parse_ping(message)
                except ValueError as error:
                    # No inventamos respuestas fuera del contrato.
                    # Ignoramos este mensaje, pero seguimos atendiendo el socket.
                    print(f"[Engine] {error}")
                    continue

                # Engine responde únicamente a cada PING recibido.
                # Monitor es quien programa las comprobaciones cada segundo.
                health = "KO" if fault_event.is_set() else "OK"
                response = f"PONG#{number}#{health}"
                send_message(connection, response)
                print(f"[Engine] {message} -> {response}")

    except KeyboardInterrupt:
        print("\n[Engine] Interrumpido por el usuario.")
    except socket.timeout:
        print("[Engine] Se agotó el tiempo para conectar o recibir la identidad.")
        return 1
    except (OSError, ValueError, UnicodeDecodeError) as error:
        print(f"[Engine] Error: {error}")
        return 1
    finally:
        # También avisamos al hilo del teclado cuando se cierra la conexión.
        # Es daemon: no impide terminar si sigue esperando un input().
        stop_event.set()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())