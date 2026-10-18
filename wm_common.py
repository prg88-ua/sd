"""Utilidades compartidas: sockets por lineas y mensajes Kafka JSON."""
import json
import socket
import time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPICS = ("fo_requests", "fo_responses", "ws_orders", "ws_events")


def field(value):
    """Un campo del contrato de sockets no puede contener separadores."""
    return isinstance(value, str) and bool(value.strip()) and not any(
        c in value for c in "#\r\n\x00"
    )


def send_line(sock, text):
    sock.sendall((text + "\n").encode("utf-8"))


class LineReader:
    """TCP puede fragmentar mensajes o agrupar varios en una lectura."""

    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def read(self, timeout=5):
        deadline = time.monotonic() + timeout

        while b"\n" not in self.buffer:
            if len(self.buffer) > 4096:
                raise ValueError("Mensaje demasiado largo")

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout()

            self.sock.settimeout(remaining)
            chunk = self.sock.recv(1024)

            if not chunk:
                raise ConnectionError("Conexion cerrada")

            self.buffer += chunk

        # Conservamos los bytes que pertenecen al siguiente mensaje.
        line, self.buffer = self.buffer.split(b"\n", 1)

        if len(line) > 4096 or b"\r" in line or b"\x00" in line:
            raise ValueError("Mensaje no valido")

        return line.decode("utf-8")


def open_bus(broker, topics):
    """Reintenta el arranque y cierra conexiones creadas a medias."""
    while True:
        producer = consumer = admin = None

        try:
            admin = KafkaAdminClient(bootstrap_servers=broker)

            for name in TOPICS:
                try:
                    admin.create_topics([NewTopic(name, 1, 1)])
                except TopicAlreadyExistsError:
                    pass

            admin.close()
            admin = None

            producer = KafkaProducer(
                bootstrap_servers=broker,
                acks="all",
                retries=5,
                max_block_ms=5000,
                request_timeout_ms=10000,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            consumer = KafkaConsumer(
                bootstrap_servers=broker,
                enable_auto_commit=False,
                group_id=None,
            )

            partitions = []

            for name in topics:
                ids = consumer.partitions_for_topic(name)

                if not ids:
                    raise RuntimeError(f"Topic no disponible: {name}")

                partitions.extend(TopicPartition(name, i) for i in ids)

            # Cada modulo recibe los mensajes y filtra por su identificador.
            # Arrancamos al final para no ejecutar ordenes antiguas.
            consumer.assign(partitions)

            # Consultar el final de cada particion y fijarlo directamente.
            # Asi empezamos con mensajes nuevos sin bloquear position().
            positions = consumer.end_offsets(partitions)

            for partition, offset in positions.items():
                consumer.seek(partition, offset)

            return producer, consumer

        except Exception as error:
            print(f"[KAFKA] Esperando a {broker}: {error}", flush=True)

            for client in (consumer, producer, admin):
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

            time.sleep(3)


def publish(producer, topic, message):
    """get confirma la entrega al broker."""
    producer.send(topic, value=message).get(timeout=12)


def messages(consumer):
    for batch in consumer.poll(timeout_ms=200).values():
        for record in batch:
            try:
                message = json.loads(record.value.decode("utf-8"))

                if isinstance(message, dict):
                    yield message

            except (UnicodeError, ValueError):
                print("[KAFKA] Mensaje mal formado ignorado", flush=True)