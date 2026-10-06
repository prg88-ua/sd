import sqlite3

# Nos conectamos a la BD
conn = sqlite3.connect('water_management.db')
cursor = conn.cursor()

# Simulamos que la WS-01 se conecta y luego se pone a regar
cursor.execute("UPDATE watering_stations SET status='AVAILABLE' WHERE id='WS-01'")
cursor.execute("UPDATE watering_stations SET status='WATERING' WHERE id='WS-02'")
cursor.execute("UPDATE watering_stations SET status='LEAK' WHERE id='WS-03'")

conn.commit()
conn.close()

print("¡Estados actualizados en la BD!")