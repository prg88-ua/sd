import socket
import time

print("[SIMULADOR] Conectando WS-01 con Central por sockets...")
s = socket.create_connection(('localhost', 6000))

# 1. Registrar la estación
s.sendall(b"REGISTRO#WS-01#Parque Central\n")
respuesta = s.recv(1024).decode().strip()
print(f"[CENTRAL] Respuesta registro: {respuesta}")

# 2. Mantener la estación viva con latidos cada segundo
print("[SIMULADOR] Enviando latidos de salud. WS-01 ahora está DISPONIBLE.")
print("Pulsa Ctrl+C cuando quieras parar la estación.\n")

try:
    while True:
        s.sendall(b"HEALTH#WS-01#OK\n")
        s.recv(1024)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[SIMULADOR] Estación desconectada.")
    s.close()