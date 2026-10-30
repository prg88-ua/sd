# Water Management - Pau y Pablo

## Lanzador con menú y terminales (Windows)

Abrir `dist/WaterManagement.exe`: elegir `fo` en PC LAB 1 y `ws` en PC LAB 2,
indicar la IP de AWS y utilizar el menú para añadir instancias, fijar un total,
recuperar por ID, abrir consolas, simular caídas y ejecutar `CLEAN`. Cada FO
abre una terminal; cada WS abre dos (Monitor y Engine). Docker Desktop debe
estar funcionando. `WaterManagement.cmd` también abre el lanzador.

Antes es necesario actualizar Central en AWS:
el lanzador da de alta IDs directamente, sin contraseña y sin reiniciar Central.
El almacenamiento existente se conserva. Una BD nueva ya no crea tres
operarios/estaciones por defecto; se registran desde el lanzador.

La [guía del lanzador](docs/LANZADOR.md) contiene los pasos completos de AWS,
uso del ejecutable, comandos equivalentes, recuperación y empaquetado.

## 1. Preparar la nube

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

Para los comandos manuales siguientes, registrar antes sus IDs con el
lanzador (`register --fo 3 --ws 3`, configurado hacia AWS), o activar
expresamente `WM_SEED_DEMO=1` al desplegar Central. El lanzador nuevo crea
automáticamente las altas necesarias al abrir cada componente.

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
- Los pedidos llevan `submitted_at`. Una petición nunca registrada que
  tenga más de 60 segundos, o no tenga fecha, se deniega para no ejecutar
  órdenes históricas al actualizar el sistema. Un ID ya registrado puede
  consultarse o reenviarse aunque sea más antiguo.

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
