import sqlite3
import os
from datetime import datetime

# Usar una ruta absoluta o relativa al directorio actual para evitar problemas
DB_NAME = os.path.join(os.path.dirname(__file__), 'water_management.db')

def get_connection():
    """Devuelve una conexión a la base de datos."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Para poder acceder a las columnas por nombre
    return conn

def init_db():
    """Inicializa las tablas de la base de datos según los requisitos de la práctica."""
    conn = get_connection()
    cursor = conn.cursor()

    # Tabla de Operarios (FO)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operators (
            id TEXT PRIMARY KEY,
            name TEXT
        )
    ''')

    # Tabla de Estaciones de Riego (WS)
    # Estados permitidos: AVAILABLE, WATERING, LEAK, OUT_OF_SERVICE, OFFLINE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watering_stations (
            id TEXT PRIMARY KEY,
            location TEXT,
            status TEXT DEFAULT 'OFFLINE',
            current_flow REAL DEFAULT 0.0,
            accumulated_volume REAL DEFAULT 0.0,
            current_operator TEXT,
            last_health_check TIMESTAMP,
            FOREIGN KEY (current_operator) REFERENCES operators (id)
        )
    ''')

    # Tabla de Historial de Riegos (Opcional pero muy útil para el panel)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS irrigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ws_id TEXT,
            operator_id TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            total_volume REAL,
            FOREIGN KEY (ws_id) REFERENCES watering_stations (id),
            FOREIGN KEY (operator_id) REFERENCES operators (id)
        )
    ''')

    # Insertar algunos datos de prueba iniciales para operadores si está vacía
    cursor.execute("SELECT COUNT(*) FROM operators")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO operators (id, name) VALUES (?, ?)",
            [('FO-01', 'Operario 1'), ('FO-02', 'Operario 2'), ('FO-03', 'Operario 3')]
        )
        
    # Insertar estaciones de prueba
    cursor.execute("SELECT COUNT(*) FROM watering_stations")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO watering_stations (id, location, status) VALUES (?, ?, ?)",
            [
                ('WS-01', 'Parque Central', 'OFFLINE'), 
                ('WS-02', 'Jardines del Río', 'OFFLINE'),
                ('WS-03', 'Plaza Mayor', 'OFFLINE')
            ]
        )

    conn.commit()
    conn.close()
    print(f"Base de datos {DB_NAME} inicializada correctamente.")

if __name__ == '__main__':
    init_db()