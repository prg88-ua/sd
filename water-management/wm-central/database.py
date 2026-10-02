import sqlite3
from datetime import datetime

DB_NAME = 'water_management.db'

def init_db():
    """Inicializa las tablas de la base de datos."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Tabla de Estaciones
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT,
            status TEXT DEFAULT 'ACTIVE',
            last_connection TIMESTAMP
        )
    ''')

    # Tabla de Lecturas (Telemetría)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            soil_moisture REAL,
            temperature REAL,
            timestamp TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations (id)
        )
    ''')

    # Tabla de Órdenes/Coordinación de Riego
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS irrigation_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            duration_minutes INTEGER,
            status TEXT DEFAULT 'PENDING', -- PENDING, IN_PROGRESS, COMPLETED
            created_at TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations (id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Base de datos inicializada correctamente.")

if __name__ == '__main__':
    init_db()