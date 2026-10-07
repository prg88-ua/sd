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

