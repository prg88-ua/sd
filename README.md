# PAU Y PABLO


# Guía de Despliegue - Water Management (Práctica SD)

## 0. Pre-requisitos en AWS (Security Groups)
Antes de empezar, asegúrate de que el Security Group de tu instancia EC2 en AWS tiene abiertos los siguientes puertos de entrada (Inbound Rules):
- **5000 (TCP):** Para el panel web (Dashboard) y la API.
- **6000 (TCP):** Para las conexiones de sockets de los Monitores.
- **9092 (TCP):** Para las conexiones externas a Kafka.

---

## 1. Despliegue en la Nube (Instancia EC2 en AWS)
Este paso levanta la Central, la base de datos SQLite y el broker de Kafka. Solo debe ejecutarse en la máquina virtual de AWS.

1. Conéctate a tu instancia EC2 (por SSH o desde la consola web EC2 Instance Connect).
2. Clona el repositorio y entra en la carpeta:
   ```bash
   git clone https://github.com/prg88-ua/sd.git
   cd sd
   ```

3. Exporta la IP pública de tu máquina EC2 para que Kafka la registre correctamente y levanta los servicios en segundo plano:
   ```bash
   export KAFKA_PUBLIC_HOST=<IP_PUBLICA_AWS>
   docker compose up -d
   ```

4. *(Opcional)* Verifica que todo está funcionando correctamente:
   ```bash
   docker compose logs -f
   ```

---

## 2. Preparación en los PCs del Laboratorio
En ambos ordenadores del laboratorio (PC 1 y PC 2), abre una terminal, clona el repositorio y construye la imagen base de Docker:

```bash
git clone https://github.com/prg88-ua/sd.git
cd sd
docker build -t water-management:local .
```

---

## 3. Despliegue de Estaciones de Riego (PC LAB 2)
Por cada estación que el profesor pida levantar (ej. `WS-01`), abre **dos terminales** en este ordenador.
*(Nota: Si usaste guiones medios en los nombres de archivo como `WM_WS-E.py`, ajusta el nombre en el comando).*

**Terminal 1 (Lanzar el Monitor):**
```bash
docker run -it --rm --network host water-management:local \
  python WM_WS/WM_WS_M/WM_WS_M.py \
  --central-host 54.216.189.86 \
  --central-port 6000 \
  --monitor-port 7001 \
  --ws-id WS-01 \
  --location "Parque_Central"
```

**Terminal 2 (Lanzar el Engine):**
```bash
docker run -it --rm --network host water-management:local \
  python WM_WS/WM_WS_E/WM_WS_E.py \
  --monitor-host 127.0.0.1 \
  --monitor-port 7001 \
  --kafka-broker 54.216.189.86:9092
```
*(Para levantar una segunda estación `WS-02`, abre otras dos terminales y cambia `--ws-id WS-02` y `--monitor-port 7002`).*

---

## 4. Despliegue de Operarios (PC LAB 1)
En el otro ordenador del laboratorio, lanza la aplicación del operario. Tienes varias opciones según lo que te pida el profesor:

**Opción A: Ejecutar riegos automáticos desde fichero**
```bash
docker run -it --rm --network host -v $(pwd)/peticiones.txt:/app/peticiones.txt:ro water-management:local \
  python WM_FO/WM_FO.py \
  --kafka-broker <IP_PUBLICA_AWS>:9092 \
  --id-operario FO-01 \
  --archivo peticiones.txt
```

**Opción B: Pedir un riego puntual manual**
```bash
docker run -it --rm --network host water-management:local \
  python WM_FO/WM_FO.py \
  --kafka-broker <IP_PUBLICA_AWS>:9092 \
  --id-operario FO-02 \
  --ws WS-01 \
  --duracion 15
```

**Opción C: Solo listar estaciones**
```bash
docker run -it --rm --network host water-management:local \
  python WM_FO/WM_FO.py \
  --kafka-broker <IP_PUBLICA_AWS>:9092 \
  --id-operario FO-03 \
  --listar
```

---

**⚠️ Un detalle importante sobre el comando del Operario con fichero:**
Fíjate en el parámetro `-v $(pwd)/peticiones.txt:/app/peticiones.txt:ro` de la **Opción A**. Como tu script FO lee el fichero desde el sistema de archivos local, este comando mapea ese archivo físico de tu PC hacia el contenedor de Docker para que pueda leerlo correctamente sin errores.
