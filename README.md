# Water Management - Pau y Pablo

## Lanzador para Windows usando el código descargado (BAT)

En los PCs del laboratorio descargad el proyecto y abrid **`WaterManagement.bat`**
con doble clic. El BAT usa el código de esa carpeta para ejecutar `docker build`
y gestionar los componentes. No contiene una copia de la aplicación.
Requiere Docker Desktop funcionando con contenedores Linux y Windows PowerShell
5.1 (incluido en Windows); no requiere Python instalado en el PC. La primera
preparación necesita Internet para descargar la imagen base y las dependencias.

Lo más sencillo es dejar el BAT dentro de `sd`, junto a `Dockerfile` y
`requirements.txt`. También detecta una subcarpeta `sd` junto al BAT. Si el
código está en otro sitio, el lanzador pide la ruta de esa carpeta. La opción
`S` permite cambiarla después; la ubicación elegida aparece sobre el menú.
No genera scripts, copias del código ni archivos auxiliares para arrancar.

Elige `fo` en el PC de operarios y `ws` en el de estaciones, e introduce la IP
pública de AWS. Los puertos por defecto son 5000 (panel), 6000 (Central) y
9092 (Kafka). El menú permite añadir componentes, ajustar el total, abrir o
recuperar un ID, abrir sus terminales, consultar el estado, detenerlos,
simular una caída, preparar la imagen y hacer CLEAN. También permite cambiar
la configuración y abrir el panel web. Por ejemplo, ajustar el total a 2 en
`ws` abre WS-01 y WS-02 con sus cuatro terminales; en `fo` abre dos operarios.

El BAT ejecuta la construcción usando la carpeta real del proyecto y su
`.dockerignore`. Detecta cambios en los archivos de la aplicación para
reconstruir la imagen al preparar o abrir componentes. Si un componente está
activo con otra versión, detenedlo antes de recuperarlo para aplicar el cambio;
su volumen se conserva. No hace falta regenerar el BAT al modificar la aplicación.
Las preferencias, incluida la ubicación del código,
se guardan en `HKCU\Software\WaterManagement\Bat`, dentro del usuario de
Windows, y los estados persistentes en volúmenes Docker por ID. CLEAN retira
solo los contenedores y la red de este lanzador; conserva los volúmenes.
Los nombres de volumen son `wm-lab-fo-FO-01-data` y
`wm-lab-engine-WS-01-data`, etc. Los volúmenes de arranques manuales anteriores
no se importan automáticamente; no arranques simultáneamente el mismo ID
desde el procedimiento manual y desde el BAT.

Al abrir FO se puede elegir un archivo de riegos o dejarlo vacío para el menú
manual. Se monta solamente el archivo elegido en lectura. Varios FO con el
mismo archivo ejecutan cada uno el lote completo. Volver a arrancarlo vuelve
a empezar ese lote, después de recuperar la petición pendiente.

**AWS también debe ejecutar esta versión de Central:** el BAT solicita las
altas idempotentes de IDs a `POST /api/catalog`. Primero actualiza el código
de AWS y reconstruye los servicios con `sudo docker compose up -d --build`.
Conserva la IP pública actual en `KAFKA_PUBLIC_HOST` (en el `.env` de AWS si
lo utilizáis). El BAT prepara los PCs, no actualiza automáticamente AWS.

Solo si se modifican las instrucciones del lanzador en `tools/launcher.ps1`,
se regenera el BAT en el PC de desarrollo:

```powershell
python tools/build_bat.py
```

El constructor solo incorpora las instrucciones del lanzador, sin empaquetar
los módulos de la práctica. `WaterManagement.bat --check` comprueba que se
encuentra una carpeta válida. También admite una ruta explícita:

```powershell
.\WaterManagement.bat --check "C:\Mi carpeta\sd"
.\WaterManagement.bat --project "C:\Mi carpeta\sd"
```

Pruebas del lanzador sin abrir ventanas ni usar contenedores reales:

```powershell
powershell -NoProfile -File tests/test_launcher.ps1
```

## 1. Preparar la nube

El panel `http://IP_AWS:5000` incluye una sección **Logs de Central** con aspecto
de terminal. Muestra solicitudes, cambios de estado, finalizaciones y errores,
con nivel y hora de Central. Se actualiza aproximadamente cada segundo, trae
solo eventos nuevos y permite pausar, seguir las últimas líneas y limpiar la
vista. Los latidos repetidos no generan líneas de estado duplicadas.

Se conservan los últimos 500 eventos en memoria del proceso; reiniciar Central
inicia una sesión de logs nueva. Limpiar la vista solo afecta al navegador.
Las lecturas usan `GET /api/logs?after=ID&session=SESION` y no generan eventos
en esta terminal. Los mensajes de la aplicación también siguen disponibles en
la salida habitual de Docker.

En el grupo de seguridad de AWS deben estar accesibles 5000/TCP (panel),
6000/TCP (Monitor-Central) y 9092/TCP (Kafka). Sustituye las IP de ejemplo
por las de tu despliegue.

```bash
git clone https://github.com/prg88-ua/sd.git
cd sd
export KAFKA_PUBLIC_HOST=<IP_PUBLICA_AWS>
docker compose up -d --build
docker compose logs -f
```

Central usa SQLite en el volumen `sqlite_data`; Kafka conserva su registro
en `kafka_data`. Reiniciar los servicios conserva ambos volúmenes.

## 2. Preparar los dos PCs del laboratorio

```bash
git clone https://github.com/prg88-ua/sd.git
cd sd
docker build -t water-management:local .
```

Los comandos siguientes usan Bash y red host (Linux, o Docker Desktop con
esa opción habilitada). En PowerShell se pueden ejecutar en una sola línea,
eliminando las barras de continuación. Usa un nombre y puerto distinto para
cada estación o contenedor adicional.

## 3. Estaciones en el PC LAB 2

Terminal 1 - Monitor de WS-01:

```bash
docker run -it --rm --name monitor-ws01 --network host water-management:local \
  python WM_WS/WM_WS_M/WM_WS_M.py \
  --central-host <IP_PUBLICA_AWS> --central-port 6000 \
  --monitor-port 7001 --ws-id WS-01 --location "Parque_Central"
```

Terminal 2 - Engine de WS-01, con su volumen persistente:

```bash
docker run -it --rm --name engine-ws01 --network host \
  --mount source=wm-engine-ws01,target=/data water-management:local \
  python WM_WS/WM_WS_E/WM_WS_E.py \
  --monitor-host 127.0.0.1 --monitor-port 7001 \
  --kafka-broker <IP_PUBLICA_AWS>:9092
```

Para WS-02 cambia los nombres a `monitor-ws02`, `engine-ws02`,
`wm-engine-ws02`, el ID a `WS-02` y el puerto del Monitor a `7002`.

El volumen debe reutilizarse al recrear el Engine. Aunque `--rm` elimina
el contenedor, el volumen con nombre conserva los checkpoints y resultados.
No arranques dos Engines simultáneos para la misma estación.

Teclas del Engine: `k` simula avería, `r` repara, `m` solicita un riego,
`p` lo detiene y `s` cierra el proceso.

## 4. Operarios en el PC LAB 1

```bash
docker run -it --rm --name fo01 --network host \
  --mount source=wm-fo01,target=/data \
  --mount type=bind,source="$(pwd)/peticiones.txt",target=/app/peticiones.txt,readonly \
  water-management:local python WM_FO/WM_FO.py \
  --kafka-broker <IP_PUBLICA_AWS>:9092 --id-operario FO-01
```

La aplicación solo recibe esos dos argumentos. Toda la interacción se hace
en el menú:

1. Consultar estaciones y recuperar el servicio pendiente.
2. Solicitar un riego manual, indicando estación y duración.
3. Leer un archivo (por defecto `peticiones.txt`).
4. Salir. Cerrar el FO no cancela un riego en curso.

Para otro operario cambia el nombre, el volumen (por ejemplo `wm-fo02`) y
`--id-operario FO-02`. Reutiliza su mismo volumen al reiniciarlo.
La opción 2 muestra las estaciones disponibles; la 1 permite ver todos los
estados.

Formato del archivo:

```text
WS-01,5
WS-02,8
WS-01,3
```

Se esperan 4 segundos entre peticiones tras éxito, denegación o error de
validación. Una línea inválida se informa y se pasa a la siguiente.
Si se pierde la respuesta y no se conoce el resultado, el lote se pausa
para no solapar servicios. La opción 1 vuelve a consultar la orden pendiente;
las líneas que aún no se enviaron no se reanudan automáticamente tras cerrar
el proceso.

## 5. Recuperación y decisiones del protocolo

- FO conserva el ID antes de enviar la orden. Si no sabe si Central la
  aceptó, reenvía ese mismo ID. Central responde con su estado sin crear
  otro riego.
- El buzón del FO conserva todas las respuestas de un lote de Kafka,
  incluida la finalización que llegue junto con la lista de estaciones.
- Central conserva pedidos, resultados, mensajes pendientes de envío y
  posiciones de lectura en SQLite. El efecto de cada mensaje y su posición
  se confirman en una única transacción.
- El Engine conserva una fila por riego en SQLite local. Un resultado
  pendiente solo se marca como entregado después del ACK de Kafka.
  Conservar su ID evita volver a abrir la válvula por un START duplicado.
- Un resultado tardío corrige su propia fila de historial; nunca borra
  las medidas de otro riego. Las últimas medidas estimadas de Central
  se identifican como provisionales.
- Las fechas son informativas. Central no rechaza una petición por
  `submitted_at` y Engine no compara `expires` con su reloj: AWS y los PCs
  pueden tener horas distintas. Los IDs persistentes siguen evitando repetir
  un riego ya registrado. Una petición nunca procesada puede aceptarse aunque
  lleve tiempo en Kafka; ya no existe un filtro de antigüedad por fecha.
- Las duraciones de riego, los latidos y los límites de espera se miden con
  reloj monotónico local: no requieren que los ordenadores tengan la misma hora.

Los archivos JSON antiguos que todavía estén en el directorio de trabajo se
importan automáticamente al nuevo estado local. La base SQLite de Central
se amplía conservando sus tablas e historiales anteriores. Los registros
antiguos de historial sin ID permanecen como estaban: no es posible asociarlos
retroactivamente a un UUID que nunca se guardó.

`WM_STATE_DIR` configura el directorio local (por defecto `.wm_state` al
ejecutar Python; `/data` en Docker). `DB_PATH` configura la BD de Central.
No se debe eliminar el almacenamiento persistente para simular una caída.

La recuperación depende de que SQLite y el registro de Kafka sigan
disponibles. Si Kafka ha eliminado por retención una posición pendiente,
Central informa del problema y no la salta silenciosamente. El sistema no
pretende recuperar datos de discos o volúmenes borrados.

## 6. Ejecución local y pruebas

Para ejecutar sin Docker durante el desarrollo:

```bash
python -m pip install -r requirements.txt
python WM_FO/WM_FO.py --kafka-broker localhost:9092 --id-operario FO-01
```

Los módulos añaden la raíz del proyecto a su ruta de importación. El
despliegue de evaluación sigue siendo el de Docker exigido por el enunciado.

Pruebas de regresión sin broker (SQLite real y fallos de red simulados):

```bash
python -m unittest discover -s tests -v
```

O con la imagen del proyecto:

```bash
docker run --rm water-management:local python -m unittest discover -s tests -v
```

Prueba distribuida recomendada en el entorno desplegado:

1. Pedir un riego largo, cerrar FO y recrearlo con su volumen.
2. Repetir, dejando que termine antes de recrear FO.
3. Interrumpir Engine en medio del riego y recrearlo con su volumen.
4. Interrumpir Central y dejar que el Engine termine; reiniciar Central.
5. Verificar el volumen y una única fila de historial por petición.
6. Enviar un lote con estación inexistente, línea inválida y servicio válido.
