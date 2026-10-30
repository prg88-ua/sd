# Pruebas con Docker simulado: no abren ventanas ni modifican contenedores reales.
$ErrorActionPreference = 'Stop'
$env:WM_LAUNCHER_TEST = '1'
. (Join-Path $PSScriptRoot '..\tools\launcher.ps1')
$script:Checks = 0

function Assert-True([bool]$Condition, [string]$Message) {
    if (!$Condition) { throw "FALLO: $Message" }
    $script:Checks++
}
function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $failed = $false
    try { & $Action } catch { $failed = $true }
    Assert-True $failed $Message
}
function Reset-Fake {
    $script:Containers = @{}
    $script:Calls = New-Object 'Collections.Generic.List[object]'
    $script:Consoles = @()
    $script:StationSettings = @{}
    $script:CentralBodies = @()
    $script:Config = @{Role='fo'; AwsHost='aws.example.org'; WebPort=5000; CentralPort=6000; KafkaPort=9092; SourceDir=''}
    $script:ProjectDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
    $script:SourceHash = 'version-simulada'
    $script:FakeImageHash = ''
}
function Open-Console([string]$Name, [string]$Part) { $script:Consoles += $Name }
function Save-Config {}
function Invoke-Central([string]$Path, [object]$Body = $null) {
    if ($null -ne $Body) { $script:CentralBodies += $Body }
    return @{ok=$true; kafka=$true}
}
function Invoke-Docker {
    param([string[]]$Arguments, [switch]$ShowProgress)
    $script:Calls.Add($Arguments)
    switch ($Arguments[0]) {
        'info' { 'linux' }
        'image' {
            if ($script:FakeImageHash) {
                if ($Arguments[1] -eq 'ls') { $script:Image }
                elseif ($Arguments[1] -eq 'inspect') {
                    @(@{Config=@{Labels=@{'wm.source'=$script:FakeImageHash}}}) | ConvertTo-Json -Depth 6
                }
            }
        }
        'build' { $script:FakeImageHash = $script:SourceHash }
        'ps' {
            $filter = $Arguments[[Array]::IndexOf($Arguments, '--filter') + 1]
            if ($filter.StartsWith('name=')) {
                $name = $filter.Substring(7).TrimEnd('$')
                if ($script:Containers.ContainsKey($name)) { $name }
            } else {
                foreach ($name in @($script:Containers.Keys)) {
                    if ($script:Containers[$name].Config.Labels.'wm.launcher' -eq $script:Owner) { $name }
                }
            }
        }
        'inspect' { @($script:Containers[$Arguments[1]]) | ConvertTo-Json -Depth 10 -Compress }
        'create' {
            $name = $Arguments[[Array]::IndexOf($Arguments, '--name') + 1]
            $labels = @{}
            for ($i=0; $i -lt $Arguments.Count; $i++) {
                if ($Arguments[$i] -eq '--label') {
                    $pair = $Arguments[$i+1] -split '=', 2
                    $labels[$pair[0]] = $pair[1]
                }
            }
            $script:Containers[$name] = [pscustomobject]@{
                Config=[pscustomobject]@{Labels=[pscustomobject]$labels; Cmd=@($Arguments[([Array]::IndexOf($Arguments,$script:Image)+1)..($Arguments.Count-1)])}
                State=[pscustomobject]@{Running=$false}
            }
            $name
        }
        'start' { $script:Containers[$Arguments[1]].State.Running=$true }
        'stop' { $script:Containers[$Arguments[-1]].State.Running=$false }
        'kill' { $script:Containers[$Arguments[1]].State.Running=$false }
        'rm' { $script:Containers.Remove($Arguments[1]) }
        'network' {
            if ($Arguments[1] -eq 'ls') { 'wm-lab' }
            elseif ($Arguments[1] -eq 'inspect') {
                @(@{Labels=@{'wm.launcher'=$script:Owner}; Containers=@{}}) | ConvertTo-Json -Depth 6
            }
        }
        default { throw "Docker simulado no contempla: $($Arguments -join ' ')" }
    }
}

Reset-Fake
Ensure-Image
Ensure-Image
$builds = @($script:Calls | Where-Object { $_[0] -eq 'build' })
Assert-True ($builds.Count -eq 1) 'reutilizar imagen si las fuentes siguen iguales'
Assert-True ($builds[0][-1] -eq $script:ProjectDir) 'docker build recibe la carpeta real del proyecto'
Assert-True ($builds[0][-1] -ne '-') 'no pasar una aplicacion empaquetada por stdin'
$script:FakeImageHash='imagen-antigua'
Ensure-Image
Assert-True (@($script:Calls | Where-Object { $_[0] -eq 'build' }).Count -eq 2) 'reconstruir cuando cambia la version'
Reset-Fake
Assert-True ((Normalize-Identity 'fo-001') -eq 'FO-01') 'normalizar ID'
Assert-Throws { Normalize-Identity 'WS-01' } 'rechazar perfil incorrecto'
Assert-Throws { Normalize-Identity 'FO-0' } 'rechazar cero'
Assert-Throws { Get-ConsoleCode 'wm; calc' 'fo' } 'no interpolar nombres arbitrarios'
$code = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String((Get-ConsoleCode 'wm-lab-fo-FO-01' 'fo')))
Assert-True ($code.Contains('docker attach --sig-proxy=false')) 'mantener teclado del FO'
$monitorCode = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String((Get-ConsoleCode 'wm-lab-monitor-WS-01' 'monitor')))
Assert-True ($monitorCode.Contains('docker attach --sig-proxy=true')) 'Ctrl+C se entrega al Monitor'
Assert-True (!$monitorCode.Contains(' -f ')) 'Monitor no es solo un visor de logs'

Start-Part 'FO-01' 'fo' 'C:\Archivos de clase\peticiones.txt'
Start-Part 'FO-01' 'fo' 'C:\Archivos de clase\peticiones.txt'
$creates = @($script:Calls | Where-Object { $_[0] -eq 'create' })
Assert-True ($creates.Count -eq 1) 'no duplicar contenedor activo'
Assert-True ($creates[0] -contains 'type=bind,source=C:\Archivos de clase\peticiones.txt,target=/app/lote.txt,readonly') 'ruta con espacios como argumento unico'
Assert-True ($creates[0] -contains 'source=wm-lab-fo-FO-01-data,target=/data') 'volumen persistente por ID'
Assert-True ($creates[0] -contains '--file') 'ejecutar lote opcional'
$script:Config.AwsHost='another.example.org'
Assert-Throws { Start-Part 'FO-01' 'fo' } 'no sustituir un proceso activo con otra configuracion'
Assert-True ($script:Containers['wm-lab-fo-FO-01'].State.Running) 'conservar el proceso rechazado'
Stop-Identity 'FO-01'
Start-Part 'FO-01' 'fo'
Assert-True (@($script:Calls | Where-Object { $_[0] -eq 'rm' }).Count -eq 1) 'recrear detenido al cambiar configuracion'

Reset-Fake
$script:Config.Role='ws'
Start-Identity 'WS-12'
Assert-True ($script:Containers.Count -eq 2) 'estacion con monitor y engine'
$creates = @($script:Calls | Where-Object { $_[0] -eq 'create' })
Assert-True ($creates[0] -contains 'WM_WS/WM_WS_M/WM_WS_M.py') 'monitor primero'
Assert-True ($creates[1] -contains 'wm-lab-monitor-WS-12') 'engine conecta por nombre Docker'
Assert-True (Test-ActiveIdentity 'WS-12') 'estacion completa activa'
Stop-Identity 'WS-12' $true 'engine'
Assert-True (!$script:Containers['wm-lab-engine-WS-12'].State.Running) 'crash engine'
Assert-True ($script:Containers['wm-lab-monitor-WS-12'].State.Running) 'crash engine conserva monitor'
Assert-True (!(Test-ActiveIdentity 'WS-12')) 'detectar estacion incompleta'
Start-Identity 'WS-12'
Assert-True ($script:Containers.Count -eq 2) 'recuperacion sin duplicados'
Assert-True ($script:Containers['wm-lab-monitor-WS-12'].State.Running) 'recuperar Engine mantiene Monitor vivo'
$script:Calls.Clear()
Stop-Identity 'WS-12'
$stops = @($script:Calls | Where-Object { $_[0] -eq 'stop' })
Assert-True ($stops[0][-1] -eq 'wm-lab-engine-WS-12') 'detener engine antes del monitor'
Assert-True ($stops[1][-1] -eq 'wm-lab-monitor-WS-12') 'monitor al final'
Clear-Local
Assert-True ($script:Containers.Count -eq 0) 'CLEAN retira contenedores propios'
Assert-True (!@($script:Calls | Where-Object { $_ -contains '-v' -or $_[0] -in @('volume','system') }).Count) 'CLEAN no borra datos ni recursos ajenos'
Assert-True ($script:StationSettings.ContainsKey('WS-12')) 'CLEAN conserva preferencias de WS'

Reset-Fake
$script:Config.Role='ws'
$station = @{Location="Parque del Rio 'Norte'"; MonitorPort=7105}
Start-Identity 'WS-25' '' $station
$monitor = $script:Containers['wm-lab-monitor-WS-25']
$engine = $script:Containers['wm-lab-engine-WS-25']
Assert-True ($monitor.Config.Cmd -contains $station.Location) 'ubicacion con espacios y comillas como un argumento'
Assert-True ($monitor.Config.Cmd -contains '7105') 'puerto elegido en Monitor'
Assert-True ($engine.Config.Cmd -contains '7105') 'Engine usa el mismo puerto elegido'
Assert-True ($script:CentralBodies[0].stations[0].location -eq $station.Location) 'alta comunica ubicacion a Central'
$beforeCalls = $script:Calls.Count
Assert-Throws { Start-Identity 'WS-25' '' @{Location='Otro parque';MonitorPort=7200} } 'no cambiar configuracion activa'
Assert-True (!@($script:Calls | Select-Object -Skip $beforeCalls | Where-Object { $_[0] -in @('create','rm','start','stop') }).Count) 'validar ambas mitades antes de cambiar contenedores'
Stop-Identity 'WS-25' $true 'monitor'
Start-Identity 'WS-25'
Assert-True ($engine.State.Running) 'recuperar Monitor mantiene Engine vivo'
Assert-True ((Get-StationSettings 'WS-25').Location -eq $station.Location) 'recuperacion conserva ubicacion'
Assert-True ((Get-StationSettings 'WS-25').MonitorPort -eq 7105) 'recuperacion conserva puerto'
Stop-Identity 'WS-25'
Start-Identity 'WS-25' '' @{Location='Parque Nuevo';MonitorPort=7200}
Assert-True ($script:Containers['wm-lab-monitor-WS-25'].Config.Cmd -contains 'Parque Nuevo') 'cambiar ubicacion con WS detenida'
Assert-True ($script:Containers['wm-lab-engine-WS-25'].Config.Cmd -contains '7200') 'cambiar puerto con WS detenida'
foreach ($port in @(0,65536)) {
    Assert-Throws { Start-Identity 'WS-26' '' @{Location='Parque';MonitorPort=$port} } 'rechazar puerto invalido'
}
foreach ($location in @('', 'Con#separador', ('x'*101), "Con`nsalto")) {
    Assert-Throws { Start-Identity 'WS-26' '' @{Location=$location;MonitorPort=7000} } 'rechazar ubicacion invalida'
}
Assert-True (!$script:Containers.ContainsKey('wm-lab-monitor-WS-26')) 'datos invalidos no crean contenedores'
# Si no hay preferencias, migrar los parametros reales del Monitor existente.
$script:StationSettings.Clear()
$imported = Get-StationSettings 'WS-25'
Assert-True ($imported.Location -eq 'Parque Nuevo' -and $imported.MonitorPort -eq 7200) 'importar ubicacion y puerto del contenedor'
$newIDs = @(Get-NewIdentities 2)
Assert-True ($newIDs.Count -eq 2 -and $newIDs -notcontains 'WS-25') 'nuevos IDs no duplican contenedores detenidos'

Reset-Fake
Start-Part 'FO-01' 'fo'
$script:Containers['wm-lab-fo-FO-01'].Config.Labels.'wm.launcher'='otro'
Assert-Throws { Stop-Identity 'FO-01' } 'no detener contenedores de otro despliegue'
Assert-Throws { Start-Part 'FO-01' 'fo' } 'no apropiarse de nombres ajenos'

# Recorrer el menu real con respuestas predefinidas, sin registro ni ventanas.
function Read-Config {}
function Save-Config {}
function Ensure-Image {}
function Ensure-Network {}
function Wait-Stations([string[]]$Identities) {}
function Read-Host([string]$Prompt) {
    if (!$script:Answers.Count) { throw "No quedan respuestas para: $Prompt" }
    return $script:Answers.Dequeue()
}
Reset-Fake
$script:Answers = New-Object 'Collections.Generic.Queue[string]'
foreach ($answer in @('1','2','','2','','3','FO-07','','0','n')) { $script:Answers.Enqueue($answer) }
$env:WM_BAT_ACTION=''
Start-Launcher
Assert-True (Test-ActiveIdentity 'FO-01') 'abrir varias crea FO-01'
Assert-True (Test-ActiveIdentity 'FO-02') 'abrir varias crea FO-02'
Assert-True (Test-ActiveIdentity 'FO-03') 'anadir una crea FO-03 sin pedir cantidad'
Assert-True (Test-ActiveIdentity 'FO-07') 'abrir por ID no reduce el resto'
Assert-True ($script:Answers.Count -eq 0) 'menu consumio todas las opciones esperadas'
$script:Answers = New-Object 'Collections.Generic.Queue[string]'
foreach ($answer in @('2','','0','n')) { $script:Answers.Enqueue($answer) }
Start-Launcher
Assert-True ((Test-ActiveIdentity 'FO-01') -and (Test-ActiveIdentity 'FO-07') -and (Test-ActiveIdentity 'FO-04')) 'anadir no reduce el resto'

Reset-Fake
$script:Config.Role='ws'
$script:Answers = New-Object 'Collections.Generic.Queue[string]'
foreach ($answer in @('1','2','Parque Central','Jardin del Rio','2','Plaza Mayor','3','WS-42','Parque Personalizado','7142','0','n')) { $script:Answers.Enqueue($answer) }
Start-Launcher
Assert-True (Test-ActiveIdentity 'WS-01') 'menu abre varias WS'
Assert-True ($script:StationSettings['WS-02'].Location -eq 'Jardin del Rio') 'ubicacion por cada WS del lote'
Assert-True ($script:StationSettings['WS-03'].Location -eq 'Plaza Mayor') 'opcion 2 pide ubicacion de una WS'
Assert-True ($script:StationSettings['WS-42'].MonitorPort -eq 7142) 'opcion 3 permite ID y puerto personalizados'
Assert-True ($script:Answers.Count -eq 0) 'menu WS consume entradas previstas'
Assert-True (!@($script:Calls | Where-Object { $_[0] -in @('stop','kill','rm') }).Count) 'abrir no detiene otras WS'
Stop-Identity 'WS-42' $true 'monitor'
$script:Answers = New-Object 'Collections.Generic.Queue[string]'
foreach ($answer in @('3','WS-42','0','n')) { $script:Answers.Enqueue($answer) }
Start-Launcher
Assert-True ($script:StationSettings['WS-42'].MonitorPort -eq 7142) 'recuperar WS activa parcial no pide ni cambia opciones'
Assert-True ($script:StationSettings['WS-42'].Location -eq 'Parque Personalizado') 'ubicacion sobrevive a recuperacion desde menu'
Assert-True (Test-ActiveIdentity 'WS-42') 'recuperacion desde menu completa la WS'
Write-Host "Correctas $script:Checks comprobaciones del lanzador."
