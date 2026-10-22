# PAU Y PABLO

docker run --name wm-monitor-01 --network wm-clientes water-management:local python WM_WS/WM_WS_M/WM_WS_M.py --central-host 54.216.189.86 --central-port 6000 --monitor-port 7001 --ws-id WS-01 --location "Parque Central" 

docker run -it --name wm-engine-01 --network wm-clientes water-management:local python WM_WS/WM_WS_E/WM_WS-E.py --monitor-host wm-monitor-01 --monitor-port 7001 --kafka-broker 54.216.189.86:9092

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --listar


# Uso: Riego ESTACION SEGUNDOS OPERARIO
function Riego {
    param(
        [string]$Estacion,
        [int]$Segundos,
        [string]$Operario = "FO-01"
    )

    docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario $Operario --ws $Estacion --duracion $Segundos
}



docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --listar

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-02 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 30

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-02 --duracion 15

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 30

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 60

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-99 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 0

pablo
docker run --rm -it --network wm-clientes water-management:local python WM_WS/WM_WS_M/WM_WS_M.py --central-host 54.216.189.86 --central-port 6000 --monitor-port 7011 --ws-id WS-01 --location "Duplicada"

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --archivo peticiones.txt

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 60

pablo
docker kill wm-monitor-01

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-02 --duracion 10

pablo
docker start -a wm-monitor-01

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 60

pablo
docker kill wm-engine-01

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-02 --duracion 10

pablo
docker start -ai wm-engine-01

docker run --rm -it --name wm-fo-prueba water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 30

docker kill wm-fo-prueba

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --listar

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 60

pablo 
cd ~/water-management
sudo docker compose stop -t 0 central

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-02 --ws WS-02 --duracion 10

pablo
sudo docker compose start central

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 60

pablo
sudo docker compose stop -t 0 broker

pablo
sudo docker compose start broker
sudo docker compose ps

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

pablo 
docker stop wm-monitor-01 wm-monitor-02
sudo docker compose restart central
sudo docker compose exec central python -c 'import sqlite3; c=sqlite3.connect("/data/water_management.db"); print(*c.execute("SELECT ws_id, operator_id, total_volume FROM irrigation_history ORDER BY id DESC LIMIT 10"), sep="\n")'
docker start -a wm-monitor-01
docker start -a wm-monitor-02

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-01 --ws WS-01 --duracion 10

pablo 
docker run --name wm-monitor-04 --network wm-clientes water-management:local python WM_WS/WM_WS_M/WM_WS_M.py --central-host 54.216.189.86 --central-port 6000 --monitor-port 7004 --ws-id WS-04 --location "Parque de pruebas"

pablo
docker run -it --name wm-engine-04 --network wm-clientes water-management:local python WM_WS/WM_WS_E/WM_WS-E.py --monitor-host wm-monitor-04 --monitor-port 7004 --kafka-broker 54.216.189.86:9092

docker run --rm -it water-management:local python WM_FO/WM-FO.py --kafka-broker 54.216.189.86:9092 --id-operario FO-03 --ws WS-04 --duracion 10

