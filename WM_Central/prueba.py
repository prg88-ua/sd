"""
prueba_engine.py — Simula un Engine mínimo para probar el Monitor.
Se conecta al Monitor, responde al IDENTIFY y contesta PONG OK/KO.
"""
import argparse
import socket
import time

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--monitor-host", default="localhost")
    p.add_argument("--monitor-port", type=int, required=True)
    p.add_argument("--fallo", action="store_true", help="Responde PONG KO (simula avería)")
    args = p.parse_args()

    print(f"[ENGINE-SIM] Conectando al Monitor en {args.monitor_host}:{args.monitor_port}…")
    sock = socket.create_connection((args.monitor_host, args.monitor_port), timeout=5)
    print("[ENGINE-SIM] Conectado.")

    # 1. Enviar IDENTIFY
    sock.sendall(b"IDENTIFY\n")
    respuesta = sock.recv(1024).decode().strip()
    print(f"[ENGINE-SIM] Recibido: {respuesta}")   # Debe ser IDENTITY#WS-01

    # 2. Responder a PINGs
    estado = "KO" if args.fallo else "OK"
    print(f"[ENGINE-SIM] Respondiendo PINGs con {estado}. Ctrl+C para parar.")
    sock.settimeout(5.0)

    try:
        while True:
            try:
                data = sock.recv(1024).decode().strip()
                if not data:
                    break
                print(f"[ENGINE-SIM] Recibido: {data}")
                partes = data.split("#")
                if partes[0] == "PING":
                    seq = partes[1]
                    pong = f"PONG#{seq}#{estado}\n"
                    sock.sendall(pong.encode())
                    print(f"[ENGINE-SIM] Enviado: {pong.strip()}")
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        print("\n[ENGINE-SIM] Detenido.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()