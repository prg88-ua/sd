# 1. Imagen base oficial ligera con Python 3.11
FROM python:3.11-slim

# 2. Evitar que Python escriba archivos .pyc y forzar salida de consola en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. Copiar e instalar las dependencias de Python
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar todo el código del proyecto al contenedor
COPY . /app/

# 6. Inicializar la base de datos si no existe
RUN python WM_Central/database.py

# 7. Exponer los puertos que utiliza Central:
#    - 5000: Servidor Web Flask (Dashboard y API REST)
#    - 6000: Servidor TCP Sockets (Monitores WM_WS_M)
EXPOSE 5000 6000

# 8. Variables de entorno por defecto (pueden ser sobreescritas en Railway o docker run)
ENV SOCKET_PORT=6000
ENV KAFKA_BROKER=localhost:9092

# 9. Comando por defecto para arrancar Central
CMD ["sh", "-c", "python WM_Central/WM_Central.py --socket-port $SOCKET_PORT --kafka-broker $KAFKA_BROKER"]