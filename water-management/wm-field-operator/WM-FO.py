import argparse
import time
import os
from kafka import KafkaProducer, KafkaConsumer

def main():
    parser = argparse.ArgumentParser(description="WM_FO - Aplicación de Operario de Campo")
    parser.add_argument("--kafka-broker", required=True, help="IP y puerto de Kafka (ej: localhost:9092)")
    parser.add_argument("--id-operario", required=True, help="ID único del operario (ej: FO-01)")
    
    # Requisitos del PDF: pedir manualmente o leer de un archivo
    parser.add_argument("--ws", type=str, help="ID de la estación (para petición manual)", default=None)
    parser.add_argument("--duracion", type=int, help="Duración del riego en segundos", default=10)
    parser.add_argument("--archivo", type=str, help="Ruta al fichero de peticiones automáticas", default=None)
    
    args = parser.parse_args()

    if not args.archivo and not args.ws:
        print("Error: Debes indicar una estación manual (--ws) o un archivo de trabajo (--archivo).")
        return

    print(f"[{args.id_operario}] Iniciando Aplicación de Operario...")
    
    # =========================================================
    # 1. INICIALIZAR KAFKA (Productor y Consumidor)
    # =========================================================
    try:
        # Productor: Habla hacia la Central (Topic: fo_requests)
        producer = KafkaProducer(
            bootstrap_servers=[args.kafka_broker],
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        
        # Consumidor: Escucha las respuestas de la Central (Topic: fo_responses)
        # Usamos un group_id propio para que este operario escuche solo sus cosas
        consumer = KafkaConsumer(
            'fo_responses',
            bootstrap_servers=[args.kafka_broker],
            group_id=f'grupo_{args.id_operario}',
            value_deserializer=lambda x: x.decode('utf-8'),
            auto_offset_reset='latest'
        )
    except Exception as e:
        print(f"Error conectando a Kafka. Asegúrate de que el contenedor de Kafka está corriendo: {e}")
        return

    # =========================================================
    # 2. CARGAR LA LISTA DE TRABAJO
    # =========================================================
    peticiones = []
    if args.archivo:
        if not os.path.exists(args.archivo):
            print(f"Error: No se encuentra el archivo {args.archivo}")
            return
        
        # Asumimos un archivo de texto donde cada línea es: WS-01,10
        with open(args.archivo, 'r') as f:
            for linea in f:
                if linea.strip():
                    partes = linea.strip().split(',')
                    if len(partes) == 2:
                        peticiones.append((partes[0].strip(), int(partes[1].strip())))
    elif args.ws:
        peticiones.append((args.ws, args.duracion))

    print(f"[{args.id_operario}] Lista de trabajo cargada: {len(peticiones)} tareas pendientes.")

    # =========================================================
    # 3. PROCESAR PETICIONES (Requisito del PDF)
    # =========================================================
    for ws_id, duracion in peticiones:
        print(f"\n---------------------------------------------------")
        print(f"[{args.id_operario}] 💧 SOLICITANDO RIEGO EN: {ws_id} (Duración: {duracion}s)")
        
        # Mensaje formato: REQUEST#<operario>#<estacion>#<segundos>
        mensaje_peticion = f"REQUEST#{args.id_operario}#{ws_id}#{duracion}"
        producer.send('fo_requests', value=mensaje_peticion)
        producer.flush()
        
        print(f"[{args.id_operario}] Petición enviada. Esperando autorización de CENTRAL...")

        # Bucle de escucha a Kafka
        riego_en_curso = False
        riego_finalizado = False

        while not riego_finalizado:
            # Revisa si hay mensajes nuevos en Kafka
            mensajes_crudos = consumer.poll(timeout_ms=1000)
            
            for topic_partition, mensajes in mensajes_crudos.items():
                for mensaje in mensajes:
                    texto = mensaje.value
                    partes = texto.split("#")
                    
                    # Filtramos para asegurarnos de que la respuesta es para nosotros
                    if len(partes) >= 3 and partes[1] == args.id_operario:
                        comando = partes[0]
                        
                        # Caso A: Central nos autoriza o deniega el arranque
                        if comando == "ACK":
                            estado = partes[2]
                            if estado == "ACEPTADO":
                                print(f"[{args.id_operario}] ✅ CENTRAL AUTORIZA EL RIEGO en {ws_id}. Válvula abierta.")
                                riego_en_curso = True
                            elif estado == "DENEGADO":
                                motivo = partes[3] if len(partes) > 3 else "Motivo desconocido"
                                print(f"[{args.id_operario}] ❌ CENTRAL DENIEGA EL RIEGO en {ws_id}. Motivo: {motivo}")
                                riego_finalizado = True # Fracaso, pasamos a la siguiente
                                
                        # Caso B: Central nos manda el ticket final (Conclusión)
                        elif comando == "END" and riego_en_curso:
                            volumen = partes[3] if len(partes) > 3 else "0.0"
                            print(f"[{args.id_operario}] 🏁 RIEGO FINALIZADO en {ws_id}. Volumen consumido: {volumen} Litros.")
                            riego_finalizado = True # Éxito, pasamos a la siguiente

        # REQUISITO DEL PDF: "tras su conclusión, esperará 4 segundos y pasará a solicitar el siguiente"
        print(f"[{args.id_operario}] ⏱️ Esperando 4 segundos de seguridad...")
        time.sleep(4)

    print(f"\n[{args.id_operario}] Todas las tareas finalizadas con éxito. Cerrando aplicación.")
    producer.close()
    consumer.close()

if __name__ == "__main__":
    main()