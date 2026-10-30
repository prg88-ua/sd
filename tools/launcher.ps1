# Fuente del BAT: PowerShell ya viene con Windows; no requiere Python local.
# El BAT solo incorpora estas instrucciones; lee la aplicacion de una carpeta.
$ErrorActionPreference = 'Stop'
$script:Owner = 'water-management-bat-v1'
$script:Image = 'water-management:bat'
$script:Network = 'wm-lab'
$script:ConfigKey = 'HKCU:\Software\WaterManagement\Bat'
$script:Config = @{ Role = 'fo'; AwsHost = ''; WebPort = 5000; CentralPort = 6000; KafkaPort = 9092; SourceDir = '' }
$script:ProjectDir = ''
$script:SourceHash = ''
$script:RequiredSources = @('Dockerfile', '.dockerignore', 'requirements.txt', 'wm_common.py', 'wm_state.py',
    'WM_Central/database.py', 'WM_Central/WM_Central.py', 'WM_Dashboard/index.html',
    'WM_FO/WM_FO.py', 'WM_WS/WM_WS_M/WM_WS_M.py', 'WM_WS/WM_WS_E/WM_WS_E.py')

function Invoke-Docker {
    param([string[]]$Arguments, [switch]$ShowProgress)
    # En Windows PowerShell, stderr de un programa nativo puede ser ErrorRecord.
    # Su codigo de salida, no el texto de progreso de Docker, decide si ha fallado.
    $ErrorActionPreference = 'Continue'
    if ($ShowProgress) {
        & $script:Docker @Arguments 2>&1 | ForEach-Object { Write-Host $_ }
        if ($LASTEXITCODE -ne 0) { throw 'No se pudo preparar la imagen Docker.' }
        return
    }
    $result = & $script:Docker @Arguments 2>&1
    $code = $LASTEXITCODE
    if ($code -ne 0) { throw "Docker: $($result -join [Environment]::NewLine)" }
    $result | ForEach-Object { $_.ToString() }
}

function Get-Container {
    param([string]$Name)
    $names = @(Invoke-Docker -Arguments @('ps', '-a', '--filter', "name=^/$Name$", '--format', '{{.Names}}'))
    if ($names -notcontains $Name) { return $null }
    $data = (Invoke-Docker -Arguments @('inspect', $Name)) -join "`n" | ConvertFrom-Json
    return @($data)[0]
}

function Get-Managed {
    $names = @(Invoke-Docker -Arguments @('ps', '-a', '--filter', "label=wm.launcher=$script:Owner", '--format', '{{.Names}}'))
    foreach ($name in $names) {
        $item = Get-Container $name
        if ($null -ne $item) { $item }
    }
}

function Read-Number {
    param([string]$Prompt, [int]$Default = -1, [int]$Maximum = 500, [int]$Minimum = 0)
    $raw = (Read-Host $Prompt).Trim()
    if (!$raw -and $Default -ge 0) { return $Default }
    $value = 0
    if (![int]::TryParse($raw, [ref]$value) -or $value -lt $Minimum -or $value -gt $Maximum) {
        throw "Introduce un numero entre $Minimum y $Maximum."
    }
    return $value
}

function Read-Config {
    if (Test-Path -LiteralPath $script:ConfigKey) {
        $saved = Get-ItemProperty -LiteralPath $script:ConfigKey
        foreach ($key in @($script:Config.Keys)) {
            if ($null -ne $saved.PSObject.Properties[$key]) { $script:Config[$key] = $saved.$key }
        }
    }
}

function Save-Config {
    # Solo preferencias de usuario: no crea archivos junto al BAT.
    New-Item -Path $script:ConfigKey -Force | Out-Null
    foreach ($key in $script:Config.Keys) {
        New-ItemProperty -LiteralPath $script:ConfigKey -Name $key -Value ([string]$script:Config[$key]) -PropertyType String -Force | Out-Null
    }
}

function Test-ProjectFolder {
    param([string]$Folder)
    if (!$Folder -or !(Test-Path -LiteralPath $Folder -PathType Container)) { return $false }
    foreach ($relative in $script:RequiredSources) {
        if (!(Test-Path -LiteralPath (Join-Path $Folder $relative) -PathType Leaf)) { return $false }
    }
    return $true
}

function Set-ProjectFolder {
    param([string]$Folder)
    $candidate = $Folder.Trim().Trim('"')
    if (!(Test-ProjectFolder $candidate)) {
        throw 'Elige la carpeta sd que contiene Dockerfile, requirements.txt y los componentes de la practica actualizada.'
    }
    $script:ProjectDir = (Resolve-Path -LiteralPath $candidate).ProviderPath
    $script:Config.SourceDir = $script:ProjectDir
}

function Find-ProjectFolder {
    param([bool]$Prompt = $true)
    if ($script:ProjectDir) { Set-ProjectFolder $script:ProjectDir; return }
    if ($env:WM_BAT_SOURCE) { Set-ProjectFolder $env:WM_BAT_SOURCE; return }
    $batDirectory = if ($env:WM_BAT_PATH) { Split-Path -Parent $env:WM_BAT_PATH } else { '' }
    $candidates = @($batDirectory)
    if ($batDirectory) { $candidates += Join-Path $batDirectory 'sd' }
    $candidates += @((Get-Location).ProviderPath, (Join-Path (Get-Location).ProviderPath 'sd'), $script:Config.SourceDir)
    foreach ($folder in $candidates) {
        if (Test-ProjectFolder $folder) { Set-ProjectFolder $folder; return }
    }
    if (!$Prompt) { throw 'No se encuentra el proyecto. Coloca el BAT dentro de sd o indica su carpeta como segundo argumento.' }
    Set-ProjectFolder (Read-Host 'Ruta de la carpeta sd con el codigo descargado')
}

function Get-ProjectHash {
    # Leer las fuentes actuales en cada preparacion permite detectar cambios sin
    # guardar una copia del codigo dentro del BAT ni generar un archivo de estado.
    $files = @($script:RequiredSources | ForEach-Object { Join-Path $script:ProjectDir $_ })
    $files += @(Get-ChildItem -LiteralPath $script:ProjectDir -Filter '*.py' -File | ForEach-Object { $_.FullName })
    foreach ($folder in @('WM_Central', 'WM_FO', 'WM_WS', 'WM_Dashboard')) {
        $files += @(Get-ChildItem -LiteralPath (Join-Path $script:ProjectDir $folder) -File -Recurse |
            Where-Object { $_.Extension -in @('.py', '.html', '.css', '.js', '.json', '.txt') } |
            ForEach-Object { $_.FullName })
    }
    $batch = Join-Path $script:ProjectDir 'peticiones.txt'
    if (Test-Path -LiteralPath $batch -PathType Leaf) { $files += $batch }
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $manifest = foreach ($file in @($files | Sort-Object -Unique)) {
            $relative = $file.Substring($script:ProjectDir.Length).Replace('\', '/')
            $digest = [BitConverter]::ToString($sha.ComputeHash([IO.File]::ReadAllBytes($file)))
            "$relative=$digest"
        }
        return [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes(($manifest -join "`n")))).Replace('-', '').ToLowerInvariant()
    } finally { $sha.Dispose() }
}

function Set-Config {
    $role = (Read-Host "Perfil fo (operarios) o ws (estaciones) [$($script:Config.Role)]").Trim().ToLowerInvariant()
    if (!$role) { $role = $script:Config.Role }
    if ($role -notin @('fo', 'ws')) { throw 'El perfil debe ser fo o ws.' }
    $awsAddress = (Read-Host "IP publica o DNS de AWS [$($script:Config.AwsHost)]").Trim()
    if (!$awsAddress) { $awsAddress = $script:Config.AwsHost }
    if ($awsAddress.Length -gt 253 -or $awsAddress -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$') {
        throw 'Introduce una IP o DNS, sin http://, espacios ni puerto.'
    }
    $web = Read-Number "Puerto del panel [$($script:Config.WebPort)]" $script:Config.WebPort 65535 1
    $central = Read-Number "Puerto TCP de Central [$($script:Config.CentralPort)]" $script:Config.CentralPort 65535 1
    $kafka = Read-Number "Puerto Kafka [$($script:Config.KafkaPort)]" $script:Config.KafkaPort 65535 1
    $script:Config = @{Role=$role; AwsHost=$awsAddress; WebPort=$web; CentralPort=$central; KafkaPort=$kafka; SourceDir=$script:ProjectDir}
    # El registro guarda solo preferencias; no se generan .env, JSON ni carpetas.
    Save-Config
    Write-Host 'Configuracion guardada en este usuario de Windows.'
}

function Invoke-Central {
    param([string]$Path, [object]$Body = $null)
    $url = 'http://{0}:{1}{2}' -f $script:Config.AwsHost, $script:Config.WebPort, $Path
    if ($null -ne $Body) {
        $json = $Body | ConvertTo-Json -Depth 8 -Compress
        return Invoke-RestMethod -Uri $url -Method Post -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($json)) -TimeoutSec 10
    }
    return Invoke-RestMethod -Uri $url -TimeoutSec 10
}

function Assert-Central {
    $health = Invoke-Central '/health'
    if (!$health.kafka) { throw 'Central no tiene Kafka preparado. Revisa AWS.' }
}

function Ensure-Image {
    Find-ProjectFolder
    $script:SourceHash = Get-ProjectHash
    $images = @(Invoke-Docker -Arguments @('image', 'ls', '--format', '{{.Repository}}:{{.Tag}}'))
    if ($images -contains $script:Image) {
        $imageData = (Invoke-Docker -Arguments @('image', 'inspect', $script:Image)) -join "`n" | ConvertFrom-Json
        if (@($imageData)[0].Config.Labels.'wm.source' -eq $script:SourceHash) { return }
    }
    Write-Host "Preparando imagen desde: $script:ProjectDir"
    # Equivale a docker build -t water-management:bat <carpeta-del-proyecto>.
    # La ruta se pasa como un argumento, incluso con espacios o tildes.
    Invoke-Docker -Arguments @('build', '--label', "wm.source=$script:SourceHash", '-t', $script:Image, $script:ProjectDir) -ShowProgress
}

function Ensure-Network {
    $networks = @(Invoke-Docker -Arguments @('network', 'ls', '--format', '{{.Name}}'))
    if ($networks -contains $script:Network) {
        $net = (Invoke-Docker -Arguments @('network', 'inspect', $script:Network)) -join "`n" | ConvertFrom-Json
        if (@($net)[0].Labels.'wm.launcher' -ne $script:Owner) {
            throw "La red $script:Network pertenece a otro despliegue."
        }
    } else {
        Invoke-Docker -Arguments @('network', 'create', '--label', "wm.launcher=$script:Owner", $script:Network) | Out-Null
    }
}

function Normalize-Identity {
    param([string]$Value)
    $prefix = if ($script:Config.Role -eq 'fo') { 'FO' } else { 'WS' }
    $text = $Value.Trim().ToUpperInvariant()
    if ($text -notmatch "^$prefix-([0-9]{1,6})$" -or [int]$Matches[1] -lt 1) {
        throw "Usa un ID como $prefix-01."
    }
    return '{0}-{1:D2}' -f $prefix, [int]$Matches[1]
}

function Read-Batch {
    if ($script:Config.Role -ne 'fo') { return '' }
    $path = (Read-Host 'Archivo de riegos (Enter para menu manual)').Trim().Trim('"')
    if (!$path) { return '' }
    $resolved = (Resolve-Path -LiteralPath $path).ProviderPath
    if (!(Test-Path -LiteralPath $resolved -PathType Leaf) -or $resolved.Contains(',')) {
        throw 'Elige un archivo existente cuya ruta no contenga comas.'
    }
    return $resolved
}

function Get-ConsoleCode {
    param([string]$Name, [string]$Part)
    # Nombre y tipo proceden de etiquetas propias y se validan antes de usarlos.
    if ($Name -notmatch '^wm-lab-(fo|monitor|engine)-(FO|WS)-[0-9]{2,6}$') { throw 'Nombre de contenedor invalido.' }
    if ($Part -notin @('fo', 'monitor', 'engine')) { throw 'Componente invalido.' }
    $command = if ($Part -eq 'monitor') {
        "& docker logs --tail 30 -f '$Name'"
    } else {
        "& docker logs --tail 20 '$Name'; & docker attach --sig-proxy=false '$Name'"
    }
    $code = "[Console]::Title='Water Management - $Name'; $command"
    return [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($code))
}

function Open-Console {
    param([string]$Name, [string]$Part)
    $encoded = Get-ConsoleCode $Name $Part
    $existing = @(Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" |
        Where-Object { $_.CommandLine -and $_.CommandLine.Contains($encoded) })
    if ($existing.Count -gt 0) { Write-Host "La terminal de $Name ya esta abierta."; return }
    # Ventana visible: el usuario maneja aqui el FO y las teclas del Engine.
    Start-Process -FilePath 'powershell.exe' -ArgumentList "-NoLogo -NoProfile -ExecutionPolicy Bypass -EncodedCommand $encoded" | Out-Null
}

function Start-Part {
    param([string]$Identity, [string]$Part, [string]$Batch = '')
    $name = "wm-lab-$Part-$Identity"
    $signature = '{0}|{1}|{2}|{3}|{4}' -f $script:Config.AwsHost, $script:Config.WebPort, $script:Config.CentralPort, $script:Config.KafkaPort, $Batch
    $existing = Get-Container $name
    if ($null -ne $existing) {
        if ($existing.Config.Labels.'wm.launcher' -ne $script:Owner) { throw "$name pertenece a otro despliegue." }
        $same = $existing.Config.Labels.'wm.source' -eq $script:SourceHash -and $existing.Config.Labels.'wm.config' -eq $signature
        if (!$same -and $existing.State.Running) { throw "Cierra $Identity antes de cambiar su imagen, IP, puertos o archivo." }
        if (!$same) { Invoke-Docker -Arguments @('rm', $name) | Out-Null; $existing = $null }
    }
    if ($null -eq $existing) {
        $argsList = @('create', '--init', '-it', '--name', $name, '--network', $script:Network,
            '--label', "wm.launcher=$script:Owner", '--label', "wm.id=$Identity", '--label', "wm.part=$Part",
            '--label', "wm.source=$script:SourceHash", '--label', "wm.config=$signature")
        if ($Part -in @('fo', 'engine')) {
            $argsList += @('--mount', "source=$name-data,target=/data")
        }
        if ($Part -eq 'fo' -and $Batch) {
            $argsList += @('--mount', "type=bind,source=$Batch,target=/app/lote.txt,readonly")
        }
        $broker = '{0}:{1}' -f $script:Config.AwsHost, $script:Config.KafkaPort
        $command = switch ($Part) {
            'fo' { @('WM_FO/WM_FO.py', '--kafka-broker', $broker, '--id-operario', $Identity) }
            'monitor' { @('WM_WS/WM_WS_M/WM_WS_M.py', '--central-host', $script:Config.AwsHost,
                '--central-port', [string]$script:Config.CentralPort, '--monitor-port', '7000',
                '--ws-id', $Identity, '--location', "Estacion_$Identity") }
            'engine' { @('WM_WS/WM_WS_E/WM_WS_E.py', '--monitor-host', "wm-lab-monitor-$Identity",
                '--monitor-port', '7000', '--kafka-broker', $broker) }
        }
        if ($Part -eq 'fo' -and $Batch) { $command += @('--file', '/app/lote.txt') }
        Invoke-Docker -Arguments ($argsList + @($script:Image, 'python') + $command) | Out-Null
        $existing = Get-Container $name
    }
    if (!$existing.State.Running) { Invoke-Docker -Arguments @('start', $name) | Out-Null }
    Open-Console $name $Part
}

function Start-Identity {
    param([string]$Identity, [string]$Batch = '')
    Write-Host "Abriendo/recuperando $Identity..."
    $body = if ($script:Config.Role -eq 'fo') {
        @{operators=@(@{id=$Identity; name="Operario_$Identity"})}
    } else { @{stations=@(@{id=$Identity; location="Estacion_$Identity"})} }
    try { Invoke-Central '/api/catalog' $body | Out-Null }
    catch { throw "No se pudo registrar $Identity. Actualiza Central en AWS con esta version. $($_.Exception.Message)" }
    if ($script:Config.Role -eq 'fo') { Start-Part $Identity 'fo' $Batch }
    else { Start-Part $Identity 'monitor'; Start-Part $Identity 'engine' }
}

function Stop-Identity {
    param([string]$Identity, [bool]$Crash = $false, [string]$Part = 'all', [bool]$Remove = $false)
    $parts = if ($Identity.StartsWith('FO-')) { @('fo') } else { @('engine', 'monitor') }
    foreach ($component in $parts) {
        if ($Part -ne 'all' -and $Part -ne $component) { continue }
        $name = "wm-lab-$component-$Identity"
        $item = Get-Container $name
        if ($null -eq $item) { continue }
        if ($item.Config.Labels.'wm.launcher' -ne $script:Owner) { throw "$name pertenece a otro despliegue." }
        if ($item.State.Running) {
            Write-Host "Cerrando $name..."
            if ($Crash) { Invoke-Docker -Arguments @('kill', $name) | Out-Null }
            else { Invoke-Docker -Arguments @('stop', '-t', '30', $name) | Out-Null }
        }
        # Nunca usar -v: el diario del Engine y el estado del FO se conservan.
        if ($Remove) { Invoke-Docker -Arguments @('rm', $name) | Out-Null }
    }
}

function Get-LocalIdentities {
    $prefix = if ($script:Config.Role -eq 'fo') { 'FO-' } else { 'WS-' }
    Get-Managed | ForEach-Object { $_.Config.Labels.'wm.id' } |
        Where-Object { $_ -and $_.StartsWith($prefix) } | Sort-Object -Unique
}

function Test-ActiveIdentity {
    param([string]$Identity)
    $parts = if ($Identity.StartsWith('FO-')) { @('fo') } else { @('monitor', 'engine') }
    foreach ($part in $parts) {
        $item = Get-Container "wm-lab-$part-$Identity"
        if ($null -eq $item -or !$item.State.Running) { return $false }
    }
    return $true
}

function Wait-Stations {
    param([string[]]$Identities)
    if ($script:Config.Role -ne 'ws' -or $Identities.Count -eq 0) { return }
    $timer = [Diagnostics.Stopwatch]::StartNew()
    do {
        $stations = @(Invoke-Central '/api/stations')
        $pending = @($Identities | Where-Object {
            $wanted = $_
            !@($stations | Where-Object { $_.id -eq $wanted -and $_.status -ne 'OFFLINE' }).Count
        })
        if (!$pending.Count) { Write-Host 'Estaciones conectadas con Central.'; return }
        Start-Sleep -Milliseconds 500
    } while ($timer.Elapsed.TotalSeconds -lt 20)
    Write-Warning "Aun esperando: $($pending -join ', '). Revisa sus terminales y la conexion con AWS."
}

function Clear-Local {
    # CLEAN incluye ambos perfiles de ESTE PC, solo con nuestras etiquetas.
    $identities = @(Get-Managed | ForEach-Object { $_.Config.Labels.'wm.id' } | Sort-Object -Unique)
    foreach ($identity in $identities) { Stop-Identity $identity $false 'all' $true }
    $networks = @(Invoke-Docker -Arguments @('network', 'ls', '--format', '{{.Name}}'))
    if ($networks -contains $script:Network) {
        $net = (Invoke-Docker -Arguments @('network', 'inspect', $script:Network)) -join "`n" | ConvertFrom-Json
        if (@($net)[0].Labels.'wm.launcher' -eq $script:Owner -and !@($net)[0].Containers.PSObject.Properties.Count) {
            Invoke-Docker -Arguments @('network', 'rm', $script:Network) | Out-Null
        }
    }
    Write-Host 'CLEAN terminado. Volumenes, imagen y preferencias conservados.'
}

function Show-Status {
    Invoke-Docker -Arguments @('ps', '-a', '--filter', "label=wm.launcher=$script:Owner", '--format', 'table {{.Names}}\t{{.Status}}') | Out-Host
    Invoke-Central '/api/stations' | Format-Table id, location, status, current_operator -AutoSize | Out-Host
}

function Start-Launcher {
    if ($env:WM_BAT_ACTION -in @('--help', '/?')) {
        Write-Host 'WaterManagement.bat: doble clic, perfil fo o ws, IP de AWS y menu.'
        Write-Host 'Requiere Windows PowerShell 5.1 y Docker Desktop con contenedores Linux.'
        Write-Host 'Usa el codigo descargado: coloca el BAT dentro de sd o selecciona su carpeta.'
        Write-Host 'No requiere Python local. --check [carpeta] comprueba las fuentes; --project carpeta abre el menu con esa ruta.'
        return
    }
    if ($env:WM_BAT_ACTION -eq '--check') {
        Find-ProjectFolder $false
        Write-Host "Proyecto correcto: $script:ProjectDir"
        Write-Host "Version de las fuentes actuales: $(Get-ProjectHash)"
        return
    }
    $script:Docker = (Get-Command docker.exe -ErrorAction Stop).Source
    $osType = (Invoke-Docker -Arguments @('info', '--format', '{{.OSType}}')) -join ''
    if ($osType -ne 'linux') { throw 'Activa los contenedores Linux en Docker Desktop.' }
    Read-Config
    Find-ProjectFolder
    if (!$script:Config.AwsHost) { Set-Config }
    else { Save-Config }
    $mutex = New-Object Threading.Mutex($false, 'Local\WaterManagementBat')
    $locked = $false
    try {
        try { $locked = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $locked = $true }
        if (!$locked) { throw 'Ya hay otro menu Water Management abierto en este PC.' }
        while ($true) {
            Write-Host "`n=== WATER MANAGEMENT | $($script:Config.Role) | AWS $($script:Config.AwsHost) ==="
            Write-Host "Codigo: $script:ProjectDir"
            Write-Host '1. Abrir mas    2. Ajustar total    3. Abrir/recuperar ID'
            Write-Host '4. Terminales   5. Estado          6. Cerrar ID'
            Write-Host '7. Simular caida  8. CLEAN local   9. Preparar imagen'
            Write-Host 'C. Configurar   S. Carpeta codigo   P. Panel web   0. Salir'
            try {
                $option = (Read-Host 'Opcion').Trim().ToUpperInvariant()
                switch ($option) {
                    {$_ -in @('1', '2', '3')} {
                        $count = 0; $selected = @()
                        if ($option -eq '3') { $selected = @(Normalize-Identity (Read-Host 'ID')) }
                        else { $count = Read-Number 'Cantidad' }
                        $batch = if ($option -eq '3' -or $count -gt 0) { Read-Batch } else { '' }
                        Assert-Central
                        if ($option -ne '2' -or $count -gt 0) { Ensure-Image; Ensure-Network }
                        $prefix = if ($script:Config.Role -eq 'fo') { 'FO' } else { 'WS' }
                        if ($option -eq '2' -and $count -gt 0) {
                            $selected = @(1..$count | ForEach-Object { '{0}-{1:D2}' -f $prefix, $_ })
                        }
                        if ($option -eq '1') {
                            for ($number = 1; $selected.Count -lt $count -and $number -le 999999; $number++) {
                                $identity = '{0}-{1:D2}' -f $prefix, $number
                                if (!(Test-ActiveIdentity $identity)) { $selected += $identity }
                            }
                        }
                        foreach ($identity in $selected) { Start-Identity $identity $batch }
                        if ($option -eq '2') {
                            foreach ($identity in @(Get-LocalIdentities)) {
                                if ($selected -notcontains $identity) { Stop-Identity $identity }
                            }
                        }
                        Wait-Stations $selected
                    }
                    '4' {
                        $identity = Normalize-Identity (Read-Host 'ID')
                        $parts = if ($identity.StartsWith('FO-')) { @('fo') } else { @('monitor', 'engine') }
                        foreach ($part in $parts) {
                            $item = Get-Container "wm-lab-$part-$identity"
                            if ($null -ne $item -and $item.Config.Labels.'wm.launcher' -eq $script:Owner -and $item.State.Running) {
                                Open-Console "wm-lab-$part-$identity" $part
                            } else { Write-Host "$identity/$part no esta activo. Usa 3 para recuperarlo." }
                        }
                    }
                    '5' { Show-Status }
                    {$_ -in @('6', '7')} {
                        $identity = Normalize-Identity (Read-Host 'ID')
                        $part = 'all'
                        if ($identity.StartsWith('WS-')) {
                            $part = (Read-Host 'Componente engine, monitor o all [all]').Trim().ToLowerInvariant()
                            if (!$part) { $part = 'all' }
                            if ($part -notin @('engine', 'monitor', 'all')) { throw 'Componente invalido.' }
                        }
                        Stop-Identity $identity ($option -eq '7') $part
                    }
                    '8' { Clear-Local }
                    '9' { Ensure-Image }
                    'C' { Set-Config }
                    'S' { Set-ProjectFolder (Read-Host 'Ruta de la carpeta sd con el codigo'); Save-Config }
                    'P' { Start-Process ('http://{0}:{1}' -f $script:Config.AwsHost, $script:Config.WebPort) | Out-Null }
                    '0' {
                        if ((Read-Host 'Cerrar componentes con CLEAN antes de salir? [s/N]').Trim().ToLowerInvariant() -eq 's') { Clear-Local }
                        return
                    }
                    default { Write-Host 'Elige una opcion del menu.' }
                }
            } catch { Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red }
        }
    } finally {
        if ($locked) { $mutex.ReleaseMutex() }
        $mutex.Dispose()
    }
}

# Las pruebas cargan las funciones sin abrir ventanas ni tocar Docker/registro.
if ($env:WM_LAUNCHER_TEST -ne '1') {
    try { Start-Launcher }
    catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host 'Abre Docker Desktop y espera a que este preparado antes de abrir el BAT.'
        if (!$env:WM_BAT_ACTION) { [void](Read-Host 'Pulsa Enter para cerrar') }
        exit 1
    }
}
