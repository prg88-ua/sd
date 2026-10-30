# Lanzador Water Management

El mismo `WaterManagement.exe` se copia a los dos PCs Windows. Al abrirlo con
doble clic aparece el menú. El ejecutable incluye Python, el lanzador y una
copia limitada de las fuentes necesarias para construir la imagen Docker.
No necesita que el PC tenga Python ni una copia del repositorio. Sí necesita
Docker Desktop funcionando con contenedores Linux y acceso a AWS.

## Reparto de la práctica

| Equipo | Perfil | Componentes |
|---|---|---|
| AWS | `cloud` | Central, Kafka y volumen SQLite de Central; panel servido por Central |
| PC LAB 1 | `fo` | Operarios de campo |
| PC LAB 2 | `ws` | Estaciones, cada una con Monitor y Engine |

AWS debe llegar desplegado a la evaluación. Los dos PCs no arrancan Central
ni Kafka. Las ventanas del lanzador son terminales locales de los contenedores
del laboratorio, no sesiones abiertas en AWS.

## 1. Actualizar AWS antes del laboratorio

Actualizar las fuentes en AWS con el procedimiento habitual del grupo, sin
borrar los volúmenes. Desde la misma carpeta y proyecto Compose utilizados
en el despliegue anterior:

```bash
export KAFKA_PUBLIC_HOST=<IP_PUBLICA_AWS>
docker compose up -d --build --wait
```

Las altas se realizan directamente desde el lanzador, sin configurar ni
introducir una contraseña. Se mantienen la validación de los IDs y el alta
idempotente en Central, sin reinicios ni sobrescritura de datos existentes.

Se mantienen los puertos actuales: `5000` para panel/API, `6000` para Monitores
y `9092` para Kafka. Deben ser accesibles desde los PCs del laboratorio; limitar
su acceso a las redes necesarias. Kafka anuncia la IP/DNS configurada mediante
`KAFKA_PUBLIC_HOST`, que debe ser accesible desde los contenedores de ambos PCs.
El alta usa el HTTP existente del laboratorio, no añade HTTPS.

La API nueva permite dar de alta IDs sin reiniciar Central. Una estación
desconocida que se conecta por sockets sigue siendo rechazada: la API
administrativa y la autenticación del Monitor son pasos distintos.

Para gestionar AWS con el propio script, Python 3 debe estar disponible allí:

```bash
python3 wm.py --role cloud --aws-host <IP_PUBLICA_AWS> configure --cloud-project sd
python3 wm.py up
python3 wm.py register --fo 20 --ws 20
python3 wm.py status
```

**El proyecto Compose debe coincidir con el que ya usa AWS** (`docker compose ls`).
`sd` es el valor inicial porque el repositorio se despliega en esa carpeta.
Cambiar el proyecto puede apuntar a otros volúmenes y no es una migración de datos.
El perfil cloud se ejecuta en AWS, no en el PC del laboratorio.

Central ya no crea automáticamente los tres FO/WS de ejemplo en una base nueva.
Los IDs se dan de alta desde el lanzador. Los registros existentes se conservan.
`WM_SEED_DEMO=1` activa expresamente las antiguas tres altas de demostración.

## 2. Abrir el ejecutable en cada PC

1. Copiar `dist/WaterManagement.exe` a una carpeta con permiso de escritura.
2. Arrancar Docker Desktop y esperar a que esté disponible.
3. Abrir el `.exe` y elegir `fo` en PC LAB 1 o `ws` en PC LAB 2.
4. Introducir la IP pública/DNS de AWS, sin `http://` ni puerto.
5. Usar la opción 9 para preparar la imagen antes de la demostración.
6. Abrir la cantidad deseada. Los IDs se dan de alta automáticamente, sin contraseña.

La configuración se guarda junto al ejecutable en `.wm_state/launcher/config.json`.
Si cambia la IP de AWS, ejecutar `configure` o editar la
configuración antes de abrir nuevos componentes. La carpeta `dist` y el estado
local no forman parte del repositorio.

En el repositorio, `WaterManagement.cmd` abre el ejecutable si está generado;
si no existe, ejecuta `wm.py` con el Python instalado en el equipo.

El menú permite añadir componentes, fijar un total, abrir/recuperar por ID,
abrir sus ventanas, consultar estado, cerrar o simular caídas y hacer `CLEAN`.
El perfil y AWS quedan guardados: no hay que introducir comandos para usarlo.

## 3. Cantidades, IDs y ventanas

- **Añadir 5** abre cinco identidades estándar que no estén activas. Puede
  recuperar IDs detenidos antes de asignar números nuevos.
- **Fijar total 15** asegura `FO-01`…`FO-15` o `WS-01`…`WS-15` y detiene los
  IDs sobrantes. Muestra qué cerrará. Las instancias completas que ya están
  activas continúan, sin repetir sus lotes ni reiniciar sus procesos.
- **Abrir por ID** crea o recupera esa identidad; no abre otra copia.
- Una estación abre dos ventanas: Monitor y Engine. Un FO abre una ventana.
- En FO se conserva el menú existente. En Engine se conservan las teclas
  `k`, `r`, `m`, `p` y `s`. El Monitor muestra sus mensajes.
- Cerrar una ventana con la X simula la caída de ese componente: el ayudante
  mata únicamente su ID de contenedor. Para una parada ordenada, usar el menú.
- Ctrl+C en una consola utiliza el cierre del componente. La opción de caída
  abrupta utiliza `docker kill`, sin convertir la prueba en un reinicio automático.
- Salir del lanzador permite dejar los componentes abiertos o cerrarlos.
  Cada ventana del `.exe` tiene un proceso independiente del menú principal.

La pareja Monitor/Engine usa una red Docker local. Cada Monitor puede usar
el puerto interno 7000 porque su contenedor tiene su propio espacio de red;
el Engine encuentra su Monitor por nombre. No se usa red host y no se publican
veinte puertos de Monitor en Windows.

En el escenario normal solo hay un PC de FO y uno de WS. Los nombres evitan
duplicados en el Docker de ese equipo, no son una exclusión global entre todos
los ordenadores. Si el profesor distribuye el mismo tipo en más máquinas, usar
IDs distintos con la opción individual; no repetir cantidades desde FO-01 en
ambos PCs. Central detecta Monitor duplicado; FO no tiene un bloqueo global.

## 4. Comandos equivalentes para automatización

```powershell
python wm.py --role fo --aws-host <IP_PUBLICA_AWS> configure
python wm.py set --count 15
python wm.py add --count 5
python wm.py start --id FO-07
python wm.py console --id FO-07
python wm.py stop --id FO-07
python wm.py clean
```

En el PC de estaciones:

```powershell
python wm.py --role ws --aws-host <IP_PUBLICA_AWS> configure
python wm.py set --count 20
python wm.py crash --id WS-07 --part engine
python wm.py start --id WS-07 --part engine
python wm.py stop --id WS-07 --part monitor
python wm.py start --id WS-07 --part monitor
python wm.py status
python wm.py clean
```

`--role`, `--aws-host`, `--state-dir` y `--no-windows` van **antes** de la
operación. `--count`, `--id`, `--part` y `--file` van después. `--no-windows`
permite automatización sin abrir ventanas. En Linux se indican los IDs para
usar `docker attach`; las ventanas automáticas se implementan para Windows.

Las cantidades son enteros entre 0 y 1000. Fijar 0 cierra todos los componentes
de ese perfil sin necesitar conexión con AWS. Un error de alta o
conexión no cierra las instancias anteriores para reducir la cantidad.
Las cifras se refieren a instancias locales activas; Central conserva los
registros históricos de las identidades apagadas.

## 5. Archivos de peticiones

Al añadir o fijar FO, el menú ofrece una ruta de archivo opcional. En blanco
mantiene el menú manual. El lanzador monta únicamente ese archivo en modo
lectura y el FO recupera primero cualquier servicio pendiente. Después ejecuta
el lote con la lógica existente y vuelve al menú.

```powershell
python wm.py add --count 3 --file "C:\Pruebas\peticiones.txt"
```

Los tres FO intentarán ejecutar el mismo archivo; no se reparte entre ellos.
Cada petición conserva su ID y se respeta la espera de cuatro segundos entre
servicios. Volver a arrancar un FO con `--file` solicita ejecutar el archivo
desde el principio tras recuperar lo pendiente; no es un cursor persistente
de líneas aún no enviadas. Para volver al menú sin repetir el archivo, abrir
el ID sin `--file`. Fijar otra cantidad no repite los lotes de FO ya activos.

## 6. CLEAN y persistencia

`CLEAN` actúa sobre los contenedores etiquetados con el despliegue y perfil
de este lanzador. No mata todos los Python, no usa `docker system prune` y
no borra volúmenes. Al detenerse un contenedor, su cliente de consola termina
y su ventana se cierra. Se conservan también los contenedores detenidos por
una prueba de caída hasta recuperarlos o ejecutar `CLEAN`.

En WS se cierra primero Engine, con margen de 30 segundos, y después Monitor.
Engine guarda el final antes del intento de envío y mantiene su emisor vivo
mientras se produce ese final. Sin ACK, el resultado sigue pendiente en disco.
Cerrar un FO no cancela el riego que ya está prestando una estación.

Los volúmenes del lanzador son estables por ID:
`wm-lab-fo-FO-01-data`, `wm-lab-engine-WS-01-data`, etc. Se reutilizan después
de `CLEAN`. No son los nombres de los volúmenes usados en los comandos manuales
antiguos: para trasladar un estado previo de esos comandos hay que migrar o
reutilizar expresamente ese volumen antes de activar el mismo ID. Los volúmenes
antiguos no se borran. No ejecutar simultáneamente un ID manual y otro del lanzador.

Para terminar la demostración, limpiar PC LAB 1 y PC LAB 2. La nube permanece
funcionando hasta que se decida pararla expresamente en AWS con su propio
perfil `cloud`. No hay control remoto implícito del otro PC ni de AWS.

## 7. Generar otra versión del ejecutable

Después de cambiar las fuentes, generar el ejecutable desde Windows:

```powershell
python -m pip install -r requirements-build.txt
python tools/build_launcher.py --onedir
.\dist\WaterManagement\WaterManagement.exe --help
python tools/build_launcher.py
.\dist\WaterManagement.exe --help
```

El empaquetado usa [PyInstaller](https://www.pyinstaller.org/en/stable/operating-mode.html).
El `.exe` lleva una instantánea de las fuentes: modificar el repositorio no
actualiza un ejecutable ya generado. La lista de empaquetado excluye `.git`,
claves, estado, SQLite y el historial privado. La imagen Docker se reutiliza
si corresponde a esas fuentes. Si cambia la imagen/configuración y una
instancia sigue viva, se pide detenerla antes de sustituirla; nunca se elimina
un contenedor activo silenciosamente.

## 8. Verificación

```powershell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Las pruebas del lanzador simulan Docker y la red; las de catálogo usan SQLite
y Flask reales sin arrancar Kafka. Verifican altas, IDs, cantidades, cierres,
preservación del estado y conflictos. Debe completarse además una prueba con
Docker Desktop y AWS reales: abrir dos FO y dos WS, usar teclas/menús, cerrar
una ventana, recuperar solo ese componente y ejecutar dos veces `CLEAN`.
Después repetir con la cantidad solicitada por el profesor y comprobar que
Central muestra el estado correcto. Un contenedor encendido no equivale por
sí solo a una estación disponible: el lanzador espera su estado en Central.

Verificación local del 29/10/2026: 68 pruebas superadas, configuración Compose
válida y ejecutable Windows generado. Se comprobó su ayuda, guardado de
configuración y entrada/salida del menú. El motor Docker no estuvo accesible;
la prueba real de contenedores, ventanas, teclado y AWS queda pendiente.
