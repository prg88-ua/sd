import argparse
import socket
import threading
import json
import sqlite3
import time
import os

from flask import Flask, jsonify
from flask_cors import CORS

# Ruta a la base de datos (reutilizando tu diseño)
DB_NAME = os.path.join(os.path.dirname(__file__), 'water_management.db')

# =====================================================================
# 1. GESTIÓN DE BASE DE DATOS
# =====================================================================
def reset_stations_to_offline():
    """
    Según el PDF: 'CENTRAL comprobará en su BD si ya tiene estaciones... 
    hasta que no conecten, las mostrará con el estado DESCONECTADA (OFFLINE)'.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("UPDATE watering_stations SET status = 'OFFLINE'")
        conn.commit()
        conn.close()
        print("[BD] Todas las estaciones han sido inicializadas en estado OFFLINE.")
    except Exception as e:
        print(f"[BD Error] No se pudo conectar a la BD: {e}")

# =====================================================================
# 2. SERVIDOR DE SOCKETS (Para los Monitores WM_WS_M)
# =====================================================================
class SocketServer(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(10) # Escucha hasta 10 conexiones simultáneas
        
    def run(self):
        print(f"[SOCKETS] Servidor escuchando en el puerto {self.port}...")
        while True:
            # Espera bloqueante hasta que un Monitor (WM_WS_M) se conecta
            client_socket, addr = self.server_socket.accept()
            print(f"[SOCKETS] Nueva conexión desde {addr}")
            
            # Lanzamos un hilo nuevo para atender a esta estación específica
            # Así el servidor principal puede seguir escuchando a otras
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.daemon = True
            client_thread.start()

    def handle_client(self, client_socket):
        """Maneja la comunicación constante (heartbeats) con un Monitor de WS"""
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break # El cliente se ha desconectado
                
                mensaje = data.decode('utf-8')
                # Aquí procesaremos el protocolo: <STX><DATA><ETX><LRC> 
                # o el formato JSON que decidamos usar.
                print(f"[SOCKETS] Recibido: {mensaje}")
                
                # Respuesta de ejemplo: ACK
                respuesta = "ACK\n"
                client_socket.send(respuesta.encode('utf-8'))
                
        except ConnectionResetError:
            print("[SOCKETS] Un monitor se ha desconectado abruptamente.")
        finally:
            client_socket.close()

# =====================================================================
# 3. GESTOR DE KAFKA (Para los Engines WM_WS_E y Operarios WM_FO)
# =====================================================================
class KafkaManager(threading.Thread):
    def __init__(self, broker_address):
        super().__init__()
        self.broker_address = broker_address
        # Aquí inicializaríamos el consumidor y productor de Kafka
        # self.consumer = KafkaConsumer(...)
        # self.producer = KafkaProducer(...)

    def run(self):
        print(f"[KAFKA] Gestor de mensajería iniciado conectando a {self.broker_address}...")
        while True:
            # Aquí irá el bucle que lee los mensajes de Kafka
            # msg = self.consumer.poll(1.0)
            # Procesar peticiones de FO o telemetría de WS_E
            time.sleep(2) # Simulación de escucha bloqueante

# =====================================================================
# 5. SERVIDOR WEB API (Para el Dashboard HTML)
# =====================================================================
app = Flask(__name__)
CORS(app) # Permite que tu archivo HTML lea los datos sin bloqueos de seguridad

@app.route('/api/stations', methods=['GET'])
def get_stations():
    """Lee las estaciones de la BD y las devuelve a la web en formato JSON"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row # Para leer como diccionario
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_stations")
        rows = cursor.fetchall()
        conn.close()
        
        # Convertimos las filas a una lista de diccionarios
        stations = [dict(row) for row in rows]
        return jsonify(stations)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

class ApiServer(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        
    def run(self):
        print(f"[API] Servidor Web para el Dashboard iniciado en el puerto {self.port}...")
        # debug=False es obligatorio si Flask corre dentro de un hilo
        app.run(host='0.0.0.0', port=self.port, debug=False, use_reloader=False)

# =====================================================================
# 4. FUNCIÓN PRINCIPAL
# =====================================================================
def main():
    # 1. Leer los parámetros por línea de comandos (Requisito del PDF)
    parser = argparse.ArgumentParser(description="Módulo CENTRAL de WaterManagement")
    parser.add_argument("--socket-port", type=int, required=True, help="Puerto de escucha del servidor de Sockets")
    parser.add_argument("--kafka-broker", type=str, required=True, help="IP y puerto del Broker de Kafka")
    
    args = parser.parse_args()

    print("==================================================")
    print("        INICIANDO WATER MANAGEMENT CENTRAL        ")
    print("==================================================")

    # 2. Inicializar el estado de la BD
    reset_stations_to_offline()

    # 3. Iniciar Servidor de Sockets en un Hilo
    socket_server = SocketServer(args.socket_port)
    socket_server.daemon = True # Si Central se cierra, este hilo también se cierra
    socket_server.start()

    # 4. Iniciar Gestor de Kafka en un Hilo
    kafka_manager = KafkaManager(args.kafka_broker)
    kafka_manager.daemon = True
    kafka_manager.start()

    api_server = ApiServer(5000)
    api_server.daemon = True
    api_server.start()

    # 5. Mantener el programa principal vivo
    try:
        while True:
            # Aquí Central podría hacer tareas periódicas de limpieza
            # o simplemente mantenerse a la espera.
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagando el sistema central...")

if __name__ == '__main__':
    main()