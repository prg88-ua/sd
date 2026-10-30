CONTRATO de la comunicación por sockets.

Todos los mensajes se envían en UTF-8, con campos separados por # y terminados en un salto de línea \n. Los campos no pueden contener # ni saltos de línea.

Registro: Monitor a central
Monitor envía:
REGISTRO#<id_estacion>#<ubicacion>
Central responde:
STATUS#OK#REGISTERED
O:
STATUS#ERROR#<motivo>
Los motivos pueden ser (principalmete):
INVALID_MESSAGE: formato incorrecto o campos vacíos
DUPLICATE_STATION: un monitor ya tiene ese ID 
Una estación registrada previamente pero desconectada puede volver a conectarse.

Identificación: Engine a Monitor
Engine envía:
IDENTIFY
Monitor responde:
IDENTITY#<id_estacion>
Si Central todavía no ha aceptado el registro:
STATUS#ERROR#NOT_REGISTERED

Salud: Monitor a Engine
Monitor envia cada segundo:
PING#<numero>
Engine responde:
PONG#<numero>#OK 
O:
PONG#<numero>#KO 
OK indica que Engine funciona correctamente. KO indica una avería. Cuando se resuelve la avería, Engine vuelve a responder OK.
El número aumenta con cada comprobación. Engine devuelve el mismo número en su respuesta PONG. Una respuesta atrasada no cuenta como respuesta a una comprobación posterior.

Salud: Monitor → Central
Monitor informa cada segundo:
HEALTH#<id_estacion>#OK (SI TODO BIEN)
O:
HEALTH#<id_estacion>#KO (SI detecta averia)
Central responde:
STATUS#OK#HEALTH_RECEIVED
Central comprueba que el ID pertenece al Monitor conectado. Si se pierde la conexion o pasan 5 sec sin informe, 
Central marca la estación como desconectada

REGLAS BASICAS DE FUNCIONAMIENTO
*Central atiende varios monitores simultáneamente.
*Desconectar un cliente no apaga Central.
*Un mensaje incorrecto no debe tirar el servidor.
*Una estación no está disponible únicamente por registrarse: su Engine debe estar conectado y responder OK.
*Una avería durante un riego debe cerrar la válvula.

Ampliación: comunicación mediante Kafka

El contrato anterior se mantiene para sockets: UTF-8, campos separados por "#" y mensajes terminados en "\n".

Kafka transporta objetos JSON codificados en UTF-8. Cada registro de Kafka contiene un objeto completo; no utiliza el delimitador de mensajes de sockets.

Cada petición tiene un "id" único. Sus respuestas, órdenes y eventos conservan ese mismo "id", para no confundir mensajes de riegos diferentes.

Los topics utilizados son:

| Topic         | Emisor → receptor                        | Tipos de mensaje 
|---------------|------------------------------------------|------------------
| "fo_requests" | FO o petición local del Engine → CENTRAL | "LIST", "REQUEST" 
| "fo_responses"| CENTRAL → FO                             | "STATIONS", "PENDING", "ACK", "DENIED", "UPDATE", "END" 
| "ws_orders"   | CENTRAL → Engine                         | "START", "STOP" 
| "ws_events"   | Engine → CENTRAL                         | "STARTED", "REJECTED", "TELEMETRY", "END" 

Campos:

- "LIST": "type", "id", "operator".
- "REQUEST": "type", "id", "operator", "ws", "duration".
- "START": los campos de REQUEST y "expires", instante de caducidad de la orden.
- "STOP": "type", "id", "ws", "reason".
- "STARTED": "type", "id", "ws".
- "REJECTED": "type", "id", "ws", "reason".
- "TELEMETRY": "type", "id", "ws", "flow", "volume", "elapsed".
- "END" del Engine: "type", "id", "ws", "flow", "volume", "elapsed", "reason".

Las respuestas de CENTRAL incluyen "id", "operator" y, cuando corresponde, "ws":

- "STATIONS" añade "stations", lista de estaciones y estados.
- "PENDING", "ACK" y "DENIED" añaden "reason".
- "UPDATE" añade "flow", "volume" y "elapsed".
- "END" añade "volume", "elapsed" y "reason".

"duration" y "elapsed" se expresan en segundos, "flow" en litros/minuto y "volume" en litros de la sesión.

CENTRAL reserva la estación antes de solicitar su arranque. Solo envía "ACK" cuando el Engine confirma "STARTED".

El Engine envía mediciones aproximadamente cada segundo y comunica la finalización, incluyendo las interrupciones. CENTRAL no calcula un final exitoso mediante un temporizador.

Si se pierde la comunicación y no puede obtenerse la medida final, CENTRAL comunica una interrupción identificada como tal y utiliza la última medición recibida.

Cada instancia consume los mensajes y filtra por estación o petición. Al arrancar no ejecuta órdenes antiguas.


## Recuperación persistente (revisión de resiliencia)

- REQUEST añade submitted_at (segundos Unix). Central deniega pedidos no
  registrados sin fecha o de más de 60 segundos. LIST admite recover_id.
- STATIONS añade active_order y last_completed, recuperados desde SQLite.
- END incluye final y updated_at en respuestas del FO. final=false identifica
  una última medida provisional; una medida posterior del Engine puede corregirla.
- Los eventos del Engine incluyen operator y start_time para mantener la
  correlación al importar checkpoints anteriores.
- request_id es único en el historial. Reenviar REQUEST/START/END con el mismo
  ID no crea un segundo riego ni duplica la fila de historial.
- Central persiste la posición por topic/partición junto con los efectos del
  mensaje y su outbox. En Engine, FINAL_PENDING permanece hasta el ACK del broker.
- Un resultado de otro ID no cambia los datos del riego actual de la estación.
- La WS mantiene sus latidos locales y completa el riego aunque caiga Central.

## Altas administrativas del lanzador

`POST /api/admin/catalog` recibe JSON con arrays opcionales `operators`
(`id`, `name`) y `stations` (`id`, `location`). Cada array admite hasta 1000
elementos. El ID admite 1-80 letras ASCII, números, guion o guion bajo. Los
textos admiten 1-200 caracteres y no pueden contener separadores de sockets.

No requiere contraseña ni cabecera de autenticación. Devuelve 400 para un lote
inválido, 413 si el cuerpo supera 1 MiB y 200 con `created`, `operators` y
`stations` cuando el alta es correcta.

Se valida todo antes de insertar. Las altas son idempotentes, no actualizan
filas existentes ni borran su historial/volumen. SQLite y la caché de Central
se coordinan bajo el mismo bloqueo, publicando en memoria tras el commit.
Esta operación no reinicia Central ni sustituye la autenticación por sockets.

FO acepta ahora `--file`/`--batch-file` como opción de arranque: primero
recupera el servicio anterior, luego procesa el archivo y vuelve al menú.
SIGTERM en FO y Engine activa el cierre habitual; una caída abrupta conserva
la recuperación mediante los diarios ya descritos.
