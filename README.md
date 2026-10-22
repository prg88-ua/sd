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
