import argparse
import socket
import threading
import sqlite3
import time
import os
from datetime import datetime

from flask import Flask, jsonify
from flask_cors import CORS

DB_NAME = os.path.join(os.path.dirname(__file__), 'water_management.db')

# =====================================================================
# 1. GESTIÓN DE BASE DE DATOS
# =====================================================================
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def reset_stations_to_offline():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE watering_stations SET status = 'OFFLINE'")
        conn.commit()
        conn.close()
        print("[BD] Todas las estaciones han sido inicializadas en estado OFFLINE.")
    except Exception as e:
        print(f"[BD Error] No se pudo conectar a la BD: {e}")

def register_station(ws_id, location):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Comprobar si la estación ya existe en la tabla
    cursor.execute("SELECT id FROM watering_stations WHERE id = ?", (ws_id,))
    exists = cursor.fetchone()
    
    now = datetime.now().isoformat()
    if exists:
        # Si ya existe, actualizamos ubicación, estado y timestamp
        cursor.execute(
            "UPDATE watering_stations SET location = ?, status = 'AVAILABLE', last_health_check = ? WHERE id = ?",
            (location, now, ws_id)
        )
    else:
        # Si es nueva, la insertamos
        cursor.execute(
            "INSERT INTO watering_stations (id, location, status, last_health_check) VALUES (?, ?, 'AVAILABLE', ?)",
            (ws_id, location, now)
        )
    conn.commit()
    conn.close()

def update_health(ws_id, status_code):
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    
    # Si el Monitor reporta OK, está disponible. Si reporta KO, avería (fuera de servicio).
    new_status = 'AVAILABLE' if status_code == 'OK' else 'OUT_OF_SERVICE'
    
    cursor.execute(
        "UPDATE watering_stations SET status = ?, last_health_check = ? WHERE id = ?",
        (new_status, now, ws_id)
    )
    conn.commit()
    conn.close()

def mark_offline_if_inactive(timeout_seconds=5):
    """Revisa si alguna estación lleva más de X segundos sin enviar latido (health check)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, last_health_check, status FROM watering_stations WHERE status != 'OFFLINE'")
    rows = cursor.fetchall()
    
    now = datetime.now()
    for row in rows:
        ws_id, last_health_str, status = row
        if last_health_str:
            try:
                last_health = datetime.fromisoformat(last_health_str)
                diff = (now - last_health).total_seconds()
                if diff > timeout_seconds:
                    cursor.execute("UPDATE watering_stations SET status = 'OFFLINE' WHERE id = ?", (ws_id,))
                    print(f"[VIGÍA] Estación {ws_id} marcada como OFFLINE (sin respuesta en {diff:.1f}s)")
            except ValueError:
                pass
    
    conn.commit()
    conn.close()

def mark_station_offline(ws_id):
    """Marca explícitamente una estación como offline (ej: al desconectarse el socket)"""
    if ws_id:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE watering_stations SET status = 'OFFLINE' WHERE id = ?", (ws_id,))
        conn.commit()
        conn.close()


# =====================================================================
# 2. SERVIDOR DE SOCKETS (Para los Monitores WM_WS_M)
# =====================================================================
class SocketServer(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Permite reutilizar el puerto rápido si cerramos el programa
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(10)
        
        # Set para vigilar que dos monitores no usen el mismo ID a la vez
        self.connected_stations = set() 
        self.lock = threading.Lock()
        
    def run(self):
        print(f"[SOCKETS] Servidor escuchando en el puerto {self.port}...")
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"[SOCKETS] Nueva conexión desde {addr}")
            
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.daemon = True
            client_thread.start()

    def handle_client(self, client_socket):
        buffer = ""
        current_ws_id = None
        
        def send_msg(msg):
            # Helper para añadir el salto de línea requerido por el contrato
            client_socket.sendall((msg + "\n").encode('utf-8'))
            
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break # Si data está vacío, el cliente cerró la conexión
                
                buffer += data.decode('utf-8')
                
                # Vamos extrayendo mensajes completos que terminen en salto de línea
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                        
                    fields = line.split("#")
                    command = fields[0]
                    
                    # ---- COMANDO: REGISTRO ----
                    if command == "REGISTRO":
                        if len(fields) != 3:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        ws_id = fields[1]
                        location = fields[2]
                        
                        # Bloqueamos para evitar condiciones de carrera si se conectan dos a la vez
                        with self.lock:
                            if ws_id in self.connected_stations:
                                send_msg("STATUS#ERROR#DUPLICATE_STATION")
                                continue
                            self.connected_stations.add(ws_id)
                            current_ws_id = ws_id
                            
                        register_station(ws_id, location)
                        send_msg("STATUS#OK#REGISTERED")
                        print(f"[SOCKETS] Estación registrada: {ws_id} en {location}")
                        
                    # ---- COMANDO: HEALTH ----
                    elif command == "HEALTH":
                        if len(fields) != 3:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        ws_id = fields[1]
                        status_code = fields[2] # OK o KO
                        
                        # Validamos que el ID coincida con el que se registró inicialmente
                        if current_ws_id != ws_id:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        update_health(ws_id, status_code)
                        send_msg("STATUS#OK#HEALTH_RECEIVED")
                        
                    # ---- COMANDO DESCONOCIDO ----
                    else:
                        send_msg("STATUS#ERROR#INVALID_MESSAGE")
                        
        except ConnectionResetError:
            print("[SOCKETS] Un monitor se ha desconectado abruptamente.")
        except Exception as e:
            print(f"[SOCKETS] Error con cliente: {e}")
        finally:
            client_socket.close()
            # Cuando el Monitor se desconecta, liberamos su ID y actualizamos la BD
            if current_ws_id:
                with self.lock:
                    if current_ws_id in self.connected_stations:
                        self.connected_stations.remove(current_ws_id)
                mark_station_offline(current_ws_id)
                print(f"[SOCKETS] Estación {current_ws_id} desconectada y liberada.")

# =====================================================================
# 3. GESTOR DE KAFKA (Para los Engines WM_WS_E y Operarios WM_FO)
# =====================================================================
class KafkaManager(threading.Thread):
    def __init__(self, broker_address):
        super().__init__()
        self.broker_address = broker_address

    def run(self):
        print(f"[KAFKA] Gestor de mensajería iniciado conectando a {self.broker_address}...")
        while True:
            # Aquí irá la lógica de Kafka más adelante
            time.sleep(2)

# =====================================================================
# 5. SERVIDOR WEB API (Para el Dashboard HTML)
# =====================================================================
app = Flask(__name__)
CORS(app)

@app.route('/api/stations', methods=['GET'])
def get_stations():
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_stations")
        rows = cursor.fetchall()
        conn.close()
        
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
        app.run(host='0.0.0.0', port=self.port, debug=False, use_reloader=False)

# =====================================================================
# 4. FUNCIÓN PRINCIPAL
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Módulo CENTRAL de WaterManagement")
    parser.add_argument("--socket-port", type=int, required=True, help="Puerto de escucha del servidor de Sockets")
    parser.add_argument("--kafka-broker", type=str, required=True, help="IP y puerto del Broker de Kafka")
    
    args = parser.parse_args()

    print("==================================================")
    print("        INICIANDO WATER MANAGEMENT CENTRAL        ")
    print("==================================================")

    # 1. Al iniciar la central, todas las estaciones de la BD pasan a OFFLINE
    reset_stations_to_offline()

    # 2. Iniciar Servidor de Sockets en un Hilo
    socket_server = SocketServer(args.socket_port)
    socket_server.daemon = True
    socket_server.start()

    # 3. Iniciar Gestor de Kafka en un Hilo
    kafka_manager = KafkaManager(args.kafka_broker)
    kafka_manager.daemon = True
    kafka_manager.start()

    # 4. Iniciar API Web Flask en un Hilo
    api_server = ApiServer(5000)
    api_server.daemon = True
    api_server.start()

    # 5. Bucle principal y VIGÍA (Timeout de 5 segundos)
    try:
        while True:
            # Cada segundo comprobamos si hay alguna estación que se haya quedado en silencio
            mark_offline_if_inactive(timeout_seconds=5)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagando el sistema central...")

if __name__ == '__main__':
    main()