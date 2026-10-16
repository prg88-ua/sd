import argparse
import socket
import threading
import sqlite3
import time
import os
from datetime import datetime

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer

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
        cursor.execute("UPDATE watering_stations SET status = 'OFFLINE', current_operator = NULL, current_flow = 0.0")
        conn.commit()
        conn.close()
        print("[BD] Todas las estaciones inicializadas en estado OFFLINE.")
    except Exception as e:
        print(f"[BD Error] No se pudo conectar a la BD: {e}")

def register_station(ws_id, location):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM watering_stations WHERE id = ?", (ws_id,))
    exists = cursor.fetchone()
    
    now = datetime.now().isoformat()
    if exists:
        cursor.execute(
            "UPDATE watering_stations SET location = ?, status = 'AVAILABLE', last_health_check = ? WHERE id = ?",
            (location, now, ws_id)
        )
    else:
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
    
    if status_code == 'KO':
        # Si hay avería, siempre pasa a LEAK y cortamos el agua
        cursor.execute(
            "UPDATE watering_stations SET status = 'LEAK', current_flow = 0.0, last_health_check = ? WHERE id = ?",
            (now, ws_id)
        )
    else:
        # Si status_code == 'OK', solo cambiamos a AVAILABLE si NO estaba regando
        cursor.execute("SELECT status FROM watering_stations WHERE id = ?", (ws_id,))
        row = cursor.fetchone()
        current_status = row[0] if row else None
        
        if current_status == 'WATERING':
            # Sigue regando, solo actualizamos el timestamp del latido
            cursor.execute(
                "UPDATE watering_stations SET last_health_check = ? WHERE id = ?",
                (now, ws_id)
            )
        else:
            # Pasa a disponible normalmente
            cursor.execute(
                "UPDATE watering_stations SET status = 'AVAILABLE', last_health_check = ? WHERE id = ?",
                (now, ws_id)
            )
            
    conn.commit()
    conn.close()
    
def mark_offline_if_inactive(timeout_seconds=5):
    """Revisa si alguna estación lleva más de X segundos sin enviar latido"""
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
                    cursor.execute("UPDATE watering_stations SET status = 'OFFLINE', current_flow = 0.0 WHERE id = ?", (ws_id,))
                    print(f"[VIGÍA] Estación {ws_id} marcada como OFFLINE (sin respuesta en {diff:.1f}s)")
            except ValueError:
                pass
    
    conn.commit()
    conn.close()

def mark_station_offline(ws_id):
    if ws_id:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE watering_stations SET status = 'OFFLINE', current_flow = 0.0 WHERE id = ?", (ws_id,))
        conn.commit()
        conn.close()

# ---- FUNCIONES DE RIEGO PARA KAFKA ----
def check_can_water(ws_id, operator_id):
    """Valida operario y disponibilidad de la estación según la BD."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Validar operario
    cursor.execute("SELECT id FROM operators WHERE id = ?", (operator_id,))
    if not cursor.fetchone():
        conn.close()
        return False, "OPERARIO_NO_REGISTRADO"

    # 2. Validar estación y su estado
    cursor.execute("SELECT status FROM watering_stations WHERE id = ?", (ws_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False, "ESTACION_NO_EXISTE"
    
    status = row[0]
    conn.close()
    
    if status != 'AVAILABLE':
        return False, f"ESTACION_ESTADO_{status}"
    
    return True, "OK"

def start_irrigation_db(ws_id, operator_id, flow=15.0):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE watering_stations SET status = 'WATERING', current_operator = ?, current_flow = ? WHERE id = ?",
        (operator_id, flow, ws_id)
    )
    conn.commit()
    conn.close()

def finish_irrigation_db(ws_id, operator_id, duration_seconds, flow=15.0):
    conn = get_db_connection()
    cursor = conn.cursor()
    volumen_litros = round(flow * (duration_seconds / 60.0), 2)
    now = datetime.now().isoformat()
    
    cursor.execute(
        """UPDATE watering_stations 
           SET status = 'AVAILABLE', 
               current_flow = 0.0, 
               current_operator = NULL, 
               accumulated_volume = accumulated_volume + ? 
           WHERE id = ?""",
        (volumen_litros, ws_id)
    )
    
    cursor.execute(
        """INSERT INTO irrigation_history (ws_id, operator_id, start_time, end_time, total_volume)
           VALUES (?, ?, ?, ?, ?)""",
        (ws_id, operator_id, now, now, volumen_litros)
    )
    conn.commit()
    conn.close()
    return volumen_litros


# =====================================================================
# 2. SERVIDOR DE SOCKETS (Para los Monitores WM_WS_M)
# =====================================================================
class SocketServer(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(10)
        self.connected_stations = set() 
        self.lock = threading.Lock()
        
    def run(self):
        print(f"[SOCKETS] Servidor escuchando en el puerto {self.port}...")
        while True:
            client_socket, addr = self.server_socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.daemon = True
            client_thread.start()

    def handle_client(self, client_socket):
        buffer = ""
        current_ws_id = None
        
        def send_msg(msg):
            client_socket.sendall((msg + "\n").encode('utf-8'))
            
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                
                buffer += data.decode('utf-8')
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                        
                    fields = line.split("#")
                    command = fields[0]
                    
                    if command == "REGISTRO":
                        if len(fields) != 3:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        ws_id = fields[1]
                        location = fields[2]
                        
                        with self.lock:
                            if ws_id in self.connected_stations:
                                send_msg("STATUS#ERROR#DUPLICATE_STATION")
                                continue
                            self.connected_stations.add(ws_id)
                            current_ws_id = ws_id
                            
                        register_station(ws_id, location)
                        send_msg("STATUS#OK#REGISTERED")
                        print(f"[SOCKETS] Estación registrada: {ws_id} en {location}")
                        
                    elif command == "HEALTH":
                        if len(fields) != 3:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        ws_id = fields[1]
                        status_code = fields[2]
                        
                        if current_ws_id != ws_id:
                            send_msg("STATUS#ERROR#INVALID_MESSAGE")
                            continue
                            
                        update_health(ws_id, status_code)
                        send_msg("STATUS#OK#HEALTH_RECEIVED")
                    else:
                        send_msg("STATUS#ERROR#INVALID_MESSAGE")
                        
        except ConnectionResetError:
            print("[SOCKETS] Monitor desconectado abruptamente.")
        except Exception as e:
            print(f"[SOCKETS] Error cliente: {e}")
        finally:
            client_socket.close()
            if current_ws_id:
                with self.lock:
                    if current_ws_id in self.connected_stations:
                        self.connected_stations.remove(current_ws_id)
                mark_station_offline(current_ws_id)
                print(f"[SOCKETS] Estación {current_ws_id} desconectada.")


# =====================================================================
# 3. GESTOR DE KAFKA (Operarios FO y Estaciones Engine)
# =====================================================================
class KafkaManager(threading.Thread):
    def __init__(self, broker_address):
        super().__init__()
        self.broker_address = broker_address
        self.producer = None
        self.consumer = None

    def run(self):
        print(f"[KAFKA] Conectando gestor de mensajería a {self.broker_address}...")
        while not self.producer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.broker_address],
                    value_serializer=lambda v: str(v).encode('utf-8')
                )
                self.consumer = KafkaConsumer(
                    'fo_requests',
                    bootstrap_servers=[self.broker_address],
                    group_id='wm_central_group',
                    value_deserializer=lambda x: x.decode('utf-8'),
                    auto_offset_reset='latest'
                )
                print("[KAFKA] Gestor conectado y a la escucha de topic 'fo_requests'.")
            except Exception as e:
                print(f"[KAFKA] Esperando a Kafka en {self.broker_address}... ({e})")
                time.sleep(3)

        for message in self.consumer:
            raw_text = message.value.strip()
            print(f"[KAFKA] Petición recibida: {raw_text}")
            
            parts = raw_text.split("#")
            command = parts[0]
            
            # Formato esperado: REQUEST#<id_operario>#<ws_id>#<duracion>
            if command == "REQUEST" and len(parts) >= 4:
                operator_id = parts[1]
                ws_id = parts[2]
                try:
                    duration = int(parts[3])
                except ValueError:
                    duration = 10

                can_water, reason = check_can_water(ws_id, operator_id)
                
                if can_water:
                    print(f"[KAFKA] ✅ Autorizando riego en {ws_id} para {operator_id} ({duration}s)")
                    start_irrigation_db(ws_id, operator_id, flow=15.0)
                    
                    # 1. Autorizar al operario
                    self.producer.send('fo_responses', value=f"ACK#{operator_id}#ACEPTADO")
                    # 2. Enviar orden de arranque a la estación
                    self.producer.send('ws_orders', value=f"START#{ws_id}#{operator_id}#{duration}")
                    self.producer.flush()

                    # Hilo simulador de duración de riego
                    def simulate_watering_end(wid, opid, dur):
                        time.sleep(dur)
                        volumen = finish_irrigation_db(wid, opid, dur, flow=15.0)
                        self.producer.send('fo_responses', value=f"END#{opid}#{volumen}")
                        self.producer.flush()
                        print(f"[KAFKA] 🏁 Riego concluido en {wid} por {opid}. Consumo: {volumen} L")

                    threading.Thread(target=simulate_watering_end, args=(ws_id, operator_id, duration), daemon=True).start()

                else:
                    print(f"[KAFKA] ❌ Denegando riego en {ws_id} a {operator_id}. Motivo: {reason}")
                    self.producer.send('fo_responses', value=f"ACK#{operator_id}#DENEGADO#{reason}")
                    self.producer.flush()


# =====================================================================
# 4. SERVIDOR WEB API Y DASHBOARD
# =====================================================================
app = Flask(__name__)
CORS(app)

# Servir el panel directamente al abrir http://localhost:5000/
@app.route('/', methods=['GET'])
def index():
    dashboard_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'wm-dashboard'))
    return send_from_directory(dashboard_dir, 'index.html')

@app.route('/api/stations', methods=['GET'])
def get_stations():
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_stations")
        rows = cursor.fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

class ApiServer(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        
    def run(self):
        print(f"[API] Servidor Web y Dashboard disponible en http://localhost:{self.port}...")
        app.run(host='0.0.0.0', port=self.port, debug=False, use_reloader=False)


# =====================================================================
# 5. FUNCIÓN PRINCIPAL
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Módulo CENTRAL de WaterManagement")
    parser.add_argument("--socket-port", type=int, required=True, help="Puerto de escucha de Sockets")
    parser.add_argument("--kafka-broker", type=str, required=True, help="IP:Puerto del Broker de Kafka")
    
    args = parser.parse_args()

    print("==================================================")
    print("        INICIANDO WATER MANAGEMENT CENTRAL        ")
    print("==================================================")

    # 1. Inicializar estados en BD
    reset_stations_to_offline()

    # 2. Iniciar Servidor de Sockets
    socket_server = SocketServer(args.socket_port)
    socket_server.daemon = True
    socket_server.start()

    # 3. Iniciar Gestor de Kafka
    kafka_manager = KafkaManager(args.kafka_broker)
    kafka_manager.daemon = True
    kafka_manager.start()

    # 4. Iniciar Servidor Web Flask (puerto 5000)
    api_server = ApiServer(5000)
    api_server.daemon = True
    api_server.start()

    # 5. Vigía de inactividad
    try:
        while True:
            mark_offline_if_inactive(timeout_seconds=5)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagando el sistema central...")

if __name__ == '__main__':
    main()