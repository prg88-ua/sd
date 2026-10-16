"""
WM_WS-E.py — Engine de Estación de Riego
Simula la electroválvula, caudalímetro, latidos hacia el Monitor y control por Kafka.
"""

import argparse
import socket
import threading
import time
from kafka import KafkaConsumer, KafkaProducer

FORMAT = "utf-8"
CONNECTION_TIMEOUT = 5.0
MAX_MESSAGE_BYTES = 4096
DEFAULT_FLOW_RATE = 15.0  # Litros / minuto


# ─────────────────────────────────────────────
# Utilidades de Socket TCP (con Monitor)
# ─────────────────────────────────────────────

def send_message(sock, message):
    sock.sendall((message + "\n").encode(FORMAT))


class LineReader:
    def __init__(self, connection):
        self.connection = connection
        self.buffer = b""

    def read_message(self):
        while True:
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

            chunk = self.connection.recv(1024)
            if not chunk:
                raise ConnectionError("Monitor ha cerrado la conexión.")
            self.buffer += chunk


def parse_identity(message):
    if message == "STATUS#ERROR#NOT_REGISTERED":
        raise ValueError("Monitor todavía no está registrado en Central.")
    fields = message.split("#")
    if len(fields) != 2 or fields[0] != "IDENTITY" or not fields[1].strip():
        raise ValueError(f"Identificación no válida: {message!r}")
    return fields[1].strip()


def parse_ping(message):
    fields = message.split("#")
    if len(fields) != 2 or fields[0] != "PING" or not fields[1].isdigit():
        raise ValueError(f"PING no válido: {message!r}")
    return fields[1]


# ─────────────────────────────────────────────
# Simulación de la Electroválvula y Riego
# ─────────────────────────────────────────────

class ValveSimulator:
    def __init__(self, station_id):
        self.station_id = station_id
        self.is_watering = False
        self.lock = threading.Lock()
        self.stop_current_irrigation = threading.Event()
        self.current_operator = None
        self.accumulated_liters = 0.0

    def start_irrigation(self, operator, duration, fault_event):
        with self.lock:
            if self.is_watering:
                print(f"[VÁLVULA] ⚠️ Ya hay un riego en curso para {self.current_operator}.")
                return
            if fault_event.is_set():
                print("[VÁLVULA] ❌ No se puede iniciar riego: La estación tiene AVERÍA (LEAK).")
                return

            self.is_watering = True
            self.current_operator = operator
            self.stop_current_irrigation.clear()

        threading.Thread(
            target=self._irrigation_loop,
            args=(operator, duration, fault_event),
            daemon=True
        ).start()

    def stop_irrigation(self, reason="Manual"):
        with self.lock:
            if self.is_watering:
                print(f"[VÁLVULA] ⛔ Orden de parada recibida ({reason}). Cerrando electroválvula...")
                self.stop_current_irrigation.set()

    def _irrigation_loop(self, operator, duration, fault_event):
        litros_por_segundo = DEFAULT_FLOW_RATE / 60.0
        litros_sesion = 0.0

        print(f"\n=======================================================")
        print(f"[VÁLVULA] 💧 ELECTROVÁLVULA ABIERTA")
        print(f"[VÁLVULA] Estación: {self.station_id} | Operario: {operator} | Duración: {duration}s")
        print(f"[VÁLVULA] Caudal: {DEFAULT_FLOW_RATE} L/min ({litros_por_segundo:.2f} L/s)")
        print(f"=======================================================\n")

        interrupted = False
        reason = "Completado"

        for sec in range(1, duration + 1):
            if self.stop_current_irrigation.is_set():
                interrupted = True
                reason = "Interrumpido por Central / STOP"
                break

            if fault_event.is_set():
                interrupted = True
                reason = "CORTE DE EMERGENCIA POR AVERÍA / FUGA"
                break

            time.sleep(1.0)
            litros_sesion += litros_por_segundo
            print(f"[RIEGO] Progreso: {sec}/{duration}s | Litros sesión: {litros_sesion:.2f} L")

        with self.lock:
            self.is_watering = False
            self.current_operator = None
            self.accumulated_liters += litros_sesion

        print(f"\n=======================================================")
        print(f"[VÁLVULA] 🔒 ELECTROVÁLVULA CERRADA")
        print(f"[VÁLVULA] Motivo: {reason}")
        print(f"[VÁLVULA] Litros entregados en la sesión: {litros_sesion:.2f} L")
        print(f"[VÁLVULA] Total histórico de la estación: {self.accumulated_liters:.2f} L")
        print(f"=======================================================\n")


# ─────────────────────────────────────────────
# Consumidor de Kafka (Órdenes de Central)
# ─────────────────────────────────────────────

def kafka_orders_thread(broker, station_id, valve, fault_event, stop_event):
    """Escucha el topic ws_orders para recibir órdenes START o STOP de Central."""
    print(f"[KAFKA] Conectando a {broker} para escuchar 'ws_orders'...")
    consumer = None

    while not stop_event.is_set() and consumer is None:
        try:
            consumer = KafkaConsumer(
                'ws_orders',
                bootstrap_servers=[broker],
                group_id=f"engine_group_{station_id}",
                value_deserializer=lambda v: v.decode(FORMAT),
                auto_offset_reset='latest'
            )
            print(f"[KAFKA] ✔ Suscrito a topic 'ws_orders'. Esperando órdenes...")
        except Exception as e:
            print(f"[KAFKA] Esperando a broker {broker}... ({e})")
            time.sleep(3.0)

    while not stop_event.is_set():
        try:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for msg in messages:
                    order = msg.value.strip()
                    parts = order.split("#")
                    cmd = parts[0]

                    if cmd == "START" and len(parts) >= 4:
                        target_ws, op_id, dur_str = parts[1], parts[2], parts[3]
                        if target_ws == station_id:
                            try:
                                dur = int(dur_str)
                            except ValueError:
                                dur = 10
                            print(f"\n[KAFKA] 📥 Recibida orden de inicio de riego: {order}")
                            valve.start_irrigation(op_id, dur, fault_event)

                    elif cmd == "STOP" and len(parts) >= 2:
                        target_ws = parts[1]
                        if target_ws in (station_id, "ALL"):
                            print(f"\n[KAFKA] 📥 Recibida orden de parada: {order}")
                            valve.stop_irrigation(reason="Orden STOP por Kafka")

        except Exception as e:
            if not stop_event.is_set():
                print(f"[KAFKA] Error en bucle de órdenes: {e}")
                time.sleep(2.0)

    if consumer:
        consumer.close()


# ─────────────────────────────────────────────
# Menú de teclado interactivo
# ─────────────────────────────────────────────

def keyboard_menu(station_id, fault_event, stop_event, valve, kafka_broker):
    """Atiende opciones locales del teclado."""
    producer = None
    if kafka_broker:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: str(v).encode(FORMAT)
            )
        except Exception:
            pass

    print("\n---------------------------------------------------------")
    print(" MENÚ ENGINE:")
    print("   k : Simular avería (Fuga / KO)")
    print("   r : Reparar avería (OK)")
    print("   m : Pedir riego manual desde la consola de la estación")
    print("   s : Salir")
    print("---------------------------------------------------------\n")

    while not stop_event.is_set():
        try:
            option = input().strip().lower()
        except EOFError:
            break

        if stop_event.is_set():
            break

        if option == "k":
            fault_event.set()
            # Si hay un riego activo, cortar la válvula inmediatamente
            valve.stop_irrigation(reason="Avería provocada manualmente (k)")
            print("\n[Engine] ⚠️ Avería activada. Válvula CERRADA y próximos PONG serán KO.")

        elif option == "r":
            fault_event.clear()
            print("\n[Engine] ✅ Avería reparada. Los próximos PONG volverán a ser OK.")

        elif option == "m":
            if fault_event.is_set():
                print("[Engine] ❌ No se puede solicitar riego: estación averiada.")
                continue

            try:
                op = input("ID Operario local (ej: FO-01): ").strip() or "LOCAL_OP"
                dur = input("Duración en segundos (ej: 10): ").strip() or "10"
                if producer:
                    peticion = f"REQUEST#{op}#{station_id}#{dur}"
                    producer.send('fo_requests', value=peticion)
                    producer.flush()
                    print(f"[Engine] 📤 Petición enviada a Central vía Kafka: {peticion}")
                else:
                    print("[Engine] ❌ Kafka no disponible para enviar la petición manual.")
            except Exception as e:
                print(f"[Engine] Error al solicitar riego: {e}")

        elif option == "s":
            stop_event.set()
            valve.stop_irrigation(reason="Apagando Engine")
            print("[Engine] Cerrando...")
            break
        else:
            print("[Engine] Opción desconocida. Utiliza: k (avería), r (reparar), m (riego local), s (salir).")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Engine de Water Management.")
    parser.add_argument("--monitor-host", default="localhost", help="IP o hostname del Monitor.")
    parser.add_argument("--monitor-port", required=True, type=int, help="Puerto del Monitor.")
    parser.add_argument("--kafka-broker", default="localhost:9092", help="Broker de Kafka (ej: localhost:9092).")
    args = parser.parse_args()

    if not 1 <= args.monitor_port <= 65535:
        parser.error("--monitor-port debe estar entre 1 y 65535.")

    fault_event = threading.Event()
    stop_event = threading.Event()

    try:
        print(f"[Engine] Conectando con Monitor en {args.monitor_host}:{args.monitor_port}...")
        with socket.create_connection(
            (args.monitor_host, args.monitor_port),
            timeout=CONNECTION_TIMEOUT
        ) as connection:
            reader = LineReader(connection)

            # 1. Identificación
            send_message(connection, "IDENTIFY")
            station_id = parse_identity(reader.read_message())
            print(f"[Engine] ✔ Identidad obtenida del Monitor: {station_id}")

            valve = ValveSimulator(station_id)

            # 2. Hilo de órdenes Kafka
            threading.Thread(
                target=kafka_orders_thread,
                args=(args.kafka_broker, station_id, valve, fault_event, stop_event),
                daemon=True
            ).start()

            # 3. Hilo de Menú interactivo de teclado
            threading.Thread(
                target=keyboard_menu,
                args=(station_id, fault_event, stop_event, valve, args.kafka_broker),
                daemon=True
            ).start()

            # 4. Bucle principal: atender PINGs del Monitor
            connection.settimeout(1.0)
            while not stop_event.is_set():
                try:
                    message = reader.read_message()
                except socket.timeout:
                    continue

                try:
                    number = parse_ping(message)
                except ValueError as err:
                    print(f"[Engine] {err}")
                    continue

                health = "KO" if fault_event.is_set() else "OK"
                response = f"PONG#{number}#{health}"
                send_message(connection, response)

    except KeyboardInterrupt:
        print("\n[Engine] Interrumpido por el usuario.")
    except socket.timeout:
        print("[Engine] Se agotó el tiempo esperando al Monitor.")
        return 1
    except (OSError, ValueError, UnicodeDecodeError) as error:
        print(f"[Engine] Error de conexión: {error}")
        return 1
    finally:
        stop_event.set()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())