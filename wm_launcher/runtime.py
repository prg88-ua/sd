"""Orquestacion Docker e inventario. AWS y cada PC tienen tareas diferentes."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile

from wm_launcher.windows import open_console

OWNER = "org.watermanagement.launcher"
PROFILE = "org.watermanagement.profile"
KIND = "org.watermanagement.kind"
ENTITY = "org.watermanagement.entity"
SPEC = "org.watermanagement.spec"
ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILES = ("Dockerfile", "docker-compose.yml", "requirements.txt", "wm_common.py",
                "wm_state.py", "peticiones.txt")
SOURCE_DIRS = ("WM_Central", "WM_WS", "WM_FO", "WM_Dashboard")


class LauncherError(RuntimeError):
    pass


def safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise LauncherError("ID invalido: usa 1-80 letras, numeros, guion o guion bajo.")
    return value


def quantity(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1000:
        raise LauncherError("La cantidad debe ser un entero entre 0 y 1000.")
    return value


def default_home():
    base = Path(sys.executable).parent if getattr(sys, "frozen", False) else ROOT
    return Path(os.environ.get("WM_LAUNCHER_HOME", base / ".wm_state" / "launcher"))


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def read_config(home):
    path = Path(home) / "config.json"
    defaults = dict(role="", aws_host="", central_port=6000, api_port=5000,
                    kafka_port=9092, namespace="wm-lab", image="water-management:local", cloud_project="sd")
    if path.exists():
        defaults.update(json.loads(path.read_text(encoding="utf-8")))
    return defaults


def validate_config(config):
    if config["role"] not in ("cloud", "fo", "ws"):
        raise LauncherError("Selecciona el perfil cloud, fo o ws.")
    # Solo un host, sin URL/ruta: las URLs se construyen de forma controlada.
    host = config["aws_host"]
    if not isinstance(host, str) or not re.fullmatch(r"[A-Za-z0-9.-]+", host):
        raise LauncherError("Indica la IP publica o nombre DNS de AWS, sin http:// ni puerto.")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,30}", config["namespace"]):
        raise LauncherError("El despliegue debe empezar por letra minuscula y usar letras, numeros o guion.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", config.get("cloud_project", "sd")):
        raise LauncherError("Nombre de proyecto Compose invalido.")
    for key in ("central_port", "api_port", "kafka_port"):
        if isinstance(config[key], bool) or not isinstance(config[key], int) or not 1 <= config[key] <= 65535:
            raise LauncherError("Puertos fuera de rango.")


@contextmanager
def operation_lock(home):
    """Evita que dos menus asignen a la vez el mismo siguiente ID en este PC."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    with (home / "operation.lock").open("a+b") as stream:
        stream.seek(0)
        stream.write(b"0")
        stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise LauncherError("Otro lanzador esta realizando una operacion en este equipo.") from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


class Runner:
    def __init__(self, config, home=None, windows=True):
        validate_config(config)
        self.config = config
        self.home = Path(home or default_home())
        self.windows = windows
        self.role = config["role"]
        self.namespace = config["namespace"]
        self.network = self.namespace + "-local"
        self._image_id = None

    def docker(self, *arguments, check=True, timeout=60, capture=True, env=None):
        try:
            result = subprocess.run(["docker", *map(str, arguments)], check=False,
                                    capture_output=capture, text=True, encoding="utf-8",
                                    errors="replace", timeout=timeout, env=env)
        except FileNotFoundError as error:
            raise LauncherError("Docker no esta instalado o no aparece en PATH.") from error
        except subprocess.TimeoutExpired as error:
            raise LauncherError("Docker no ha respondido a tiempo.") from error
        if check and result.returncode:
            raise LauncherError((result.stderr or result.stdout or "Docker ha fallado.").strip())
        return result

    def check_docker(self):
        result = self.docker("info", "--format", "{{.OSType}}", check=False, timeout=15)
        if result.returncode:
            raise LauncherError("Docker no esta funcionando. Abre Docker Desktop y espera a que arranque.")
        if result.stdout.strip() != "linux":
            raise LauncherError("Este proyecto utiliza contenedores Linux. Cambia Docker a Linux containers.")

    def api(self, path, body=None):
        headers = {"Content-Type": "application/json"}
        url = f"http://{self.config['aws_host']}:{self.config['api_port']}{path}"
        request = urllib.request.Request(url, headers=headers,
                                         data=None if body is None else json.dumps(body).encode("utf-8"))
        # La conexión al servidor de la práctica es directa, sin proxy local.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=8) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404 and path == "/api/admin/catalog":
                raise LauncherError("Actualiza y reconstruye Central en AWS: no ofrece la API de altas.") from None
            raise LauncherError(f"Central devuelve HTTP {error.code} en {path}.") from None
        except (OSError, ValueError) as error:
            raise LauncherError("No se puede conectar con Central en AWS. Comprueba IP, puerto y red.") from error

    def register(self, operators=(), stations=()):
        # Registrar no equivale a autenticar una estacion: el Monitor sigue haciendo
        # REGISTRO por sockets y Central sigue rechazando IDs desconocidos.
        payload = {"operators": [{"id": safe_id(x), "name": "Operario " + x} for x in operators],
                   "stations": [{"id": safe_id(x), "location": "Zona " + x} for x in stations]}
        return self.api("/api/admin/catalog", payload)

    def inventory(self):
        result = self.docker("ps", "--all", "--quiet", "--filter", f"label={OWNER}={self.namespace}",
                             "--filter", f"label={PROFILE}={self.role}")
        ids = result.stdout.split()
        if not ids:
            return []
        return json.loads(self.docker("inspect", *ids).stdout)

    def inspect(self, name):
        result = self.docker("inspect", "--type", "container", name, check=False)
        if result.returncode:
            if "No such" not in result.stderr:
                raise LauncherError((result.stderr or "No se puede inspeccionar el contenedor.").strip())
            return None
        return json.loads(result.stdout)[0]

    def verify_owner(self, container):
        labels = container["Config"].get("Labels") or {}
        if labels.get(OWNER) != self.namespace or labels.get(PROFILE) != self.role:
            raise LauncherError("Hay un contenedor con ese nombre ajeno a este perfil. No se modificara.")

    def source(self):
        if not getattr(sys, "frozen", False):
            return ROOT
        archive = Path(sys._MEIPASS) / "app_src.zip"
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()[:16]
        target = self.home / "sources" / digest
        if not (target / ".complete").exists():
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(archive) as zipped:
                for member in zipped.infolist():
                    destination = (target / member.filename).resolve()
                    if not destination.is_relative_to(target.resolve()):
                        raise LauncherError("El paquete contiene una ruta fuera de su carpeta.")
                zipped.extractall(target)
            (target / ".complete").touch()
        return target

    def source_digest(self):
        source = self.source()
        files = [source / x for x in SOURCE_FILES]
        files += [p for d in SOURCE_DIRS for p in (source / d).rglob("*")
                  if p.is_file() and p.suffix in (".py", ".html", ".css", ".js")]
        digest = hashlib.sha256()
        for path in sorted(files):
            digest.update(path.relative_to(source).as_posix().encode())
            digest.update(path.read_bytes())
        return digest.hexdigest()

    def ensure_image(self, force=False):
        if self._image_id and not force:
            return self._image_id
        digest = self.source_digest()
        result = self.docker("image", "inspect", self.config["image"], check=False)
        current = json.loads(result.stdout)[0] if result.returncode == 0 else None
        if force or not current or (current["Config"].get("Labels") or {}).get(OWNER + ".source") != digest:
            print("Preparando imagen Docker (se reutilizara para todas las instancias)...", flush=True)
            self.docker("build", "--label", OWNER + ".source=" + digest,
                        "--tag", self.config["image"], self.source(), timeout=900, capture=False)
            current = json.loads(self.docker("image", "inspect", self.config["image"]).stdout)[0]
        self._image_id = current["Id"]
        return self._image_id

    def ensure_network(self):
        result = self.docker("network", "inspect", self.network, check=False)
        if result.returncode == 0:
            labels = json.loads(result.stdout)[0].get("Labels") or {}
            if labels.get(OWNER) != self.namespace:
                raise LauncherError("La red local existe pero no pertenece al lanzador.")
        else:
            self.docker("network", "create", "--label", f"{OWNER}={self.namespace}", self.network)

    def container_name(self, kind, entity):
        return f"{self.namespace}-{kind}-{safe_id(entity)}"

    def start_container(self, kind, entity, command, mounts=()):
        image_id = self.ensure_image()
        name = self.container_name(kind, entity)
        spec = hashlib.sha256(json.dumps([image_id, command, list(mounts)], sort_keys=True).encode()).hexdigest()
        container = self.inspect(name)
        if container:
            self.verify_owner(container)
            previous = (container["Config"].get("Labels") or {}).get(SPEC)
            if previous != spec:
                if container["State"]["Running"]:
                    raise LauncherError(f"{name} usa otra configuracion/imagen. Detenlo y vuelve a abrirlo.")
                self.docker("rm", container["Id"])
                container = None
        if container is None:
            args = ["run", "--detach", "--init", "--name", name, "--network", self.network,
                    "--stop-timeout", "30", "--restart", "no", "--log-opt", "max-size=10m",
                    "--log-opt", "max-file=3"]
            if kind in ("fo", "engine"):
                args += ["--interactive", "--tty"]
            for key, value in ((OWNER, self.namespace), (PROFILE, self.role), (KIND, kind),
                               (ENTITY, entity), (SPEC, spec)):
                args += ["--label", f"{key}={value}"]
            if kind in ("fo", "engine"):
                # El volumen sobrevive tanto a CLEAN como al borrado del contenedor.
                args += ["--volume", f"{self.namespace}-{kind}-{entity}-data:/data"]
            for mount in mounts:
                args += ["--mount", mount]
            args += [image_id, "python", *command]
            self.docker(*args)
            container = self.inspect(name)
        elif not container["State"]["Running"]:
            self.docker("start", container["Id"])
            container = self.inspect(name)
        open_console(container, self.home / "consoles", self.windows)
        print("[ABIERTO] " + name, flush=True)

    def start_ids(self, ids, part="both", batch_file=None):
        if self.role not in ("fo", "ws"):
            raise LauncherError("AWS usa 'up'. Los perfiles fo/ws abren componentes del laboratorio.")
        ids = [safe_id(x) for x in ids]
        if not ids:
            return
        if self.role == "fo" and part != "both":
            raise LauncherError("Un FO no tiene Monitor ni Engine.")
        mounts = []
        extra = []
        if batch_file:
            if self.role != "fo":
                raise LauncherError("Solo los FO ejecutan archivos de peticiones.")
            path = Path(batch_file).resolve()
            if not path.is_file() or "," in str(path):
                raise LauncherError("Archivo inexistente o carpeta con coma no compatible con Docker mount.")
            mounts = [f"type=bind,source={path},target=/requests/batch.txt,readonly"]
            extra = ["--file", "/requests/batch.txt"]
        self.check_docker()
        # Toda el alta se valida antes de crear el primer contenedor.
        self.api("/health")
        self.register(operators=ids if self.role == "fo" else (),
                      stations=ids if self.role == "ws" else ())
        self.ensure_image()
        self.ensure_network()
        host = self.config["aws_host"]
        broker = f"{host}:{self.config['kafka_port']}"
        for entity in ids:
            if self.role == "fo":
                self.start_container("fo", entity, ["WM_FO/WM_FO.py", "--kafka-broker", broker,
                                                    "--id-operario", entity, *extra], mounts)
            else:
                monitor = self.container_name("monitor", entity)
                if part in ("both", "monitor"):
                    self.start_container("monitor", entity, ["WM_WS/WM_WS_M/WM_WS_M.py",
                                         "--central-host", host, "--central-port", str(self.config["central_port"]),
                                         "--monitor-port", "7000", "--ws-id", entity,
                                         "--location", "Zona " + entity])
                if part in ("both", "engine"):
                    self.start_container("engine", entity, ["WM_WS/WM_WS_E/WM_WS_E.py",
                                         "--monitor-host", monitor, "--monitor-port", "7000",
                                         "--kafka-broker", broker])
        if self.role == "ws" and part != "monitor":
            self.wait_stations(ids)

    def wait_stations(self, ids, timeout=35):
        deadline = time.monotonic() + timeout
        pending = set(ids)
        while pending and time.monotonic() < deadline:
            snapshots = self.api("/api/stations")
            pending = set(ids) - {x["id"] for x in snapshots
                                  if x["status"] in ("AVAILABLE", "WATERING", "OUT_OF_SERVICE")}
            if pending:
                time.sleep(1)
        if pending:
            raise LauncherError("Se abrieron los contenedores, pero estas estaciones no estan listas: "
                                + ", ".join(sorted(pending)) + ". Consulta sus terminales/status.")

    def active_ids(self, inventory=None):
        return {x["Config"]["Labels"][ENTITY] for x in (self.inventory() if inventory is None else inventory)
                if x["State"]["Running"]}

    def next_ids(self, count):
        quantity(count)
        if self.role not in ("fo", "ws"):
            raise LauncherError("add se utiliza en el PC de FO o de estaciones.")
        active = self.active_ids()
        prefix = "FO" if self.role == "fo" else "WS"
        selected = []
        number = 1
        while len(selected) < count:
            candidate = f"{prefix}-{number:02d}"
            if candidate not in active:
                selected.append(candidate)
            number += 1
        return selected

    def set_count(self, count, batch_file=None):
        quantity(count)
        if self.role not in ("fo", "ws"):
            raise LauncherError("set/add se utiliza en el PC de FO o de estaciones.")
        prefix = "FO" if self.role == "fo" else "WS"
        desired = [f"{prefix}-{n:02d}" for n in range(1, count + 1)]
        inventory = self.inventory()
        active = self.active_ids(inventory)
        extras = active - set(desired)
        if extras:
            print("Se cerraran: " + ", ".join(sorted(extras)), flush=True)
        # Las instancias completas no se reconfiguran ni vuelven a ejecutar un lote.
        # Una pareja WS incompleta si necesita recuperar el componente que falta.
        required = {"fo"} if self.role == "fo" else {"engine", "monitor"}
        complete = {entity for entity in active if required <= {
            x["Config"]["Labels"].get(KIND) for x in inventory
            if x["State"]["Running"] and x["Config"]["Labels"].get(ENTITY) == entity}}
        missing = [entity for entity in desired if entity not in complete]
        # Primero abrir lo solicitado: un error de AWS no cierra los anteriores.
        self.start_ids(missing, batch_file=batch_file)
        for entity in sorted(extras):
            self.stop_id(entity)

    def stop_id(self, entity, part="both", crash=False):
        if self.role not in ("fo", "ws"):
            raise LauncherError("Los componentes de AWS se gestionan con up/clean.")
        entity = safe_id(entity)
        kinds = ({"fo"} if self.role == "fo" else
                 ({"engine", "monitor"} if part == "both" else {part}))
        selected = [x for x in self.inventory() if x["Config"]["Labels"].get(ENTITY) == entity
                    and x["Config"]["Labels"].get(KIND) in kinds and x["State"]["Running"]]
        # Mantener el Monitor mientras el Engine persiste y publica su END.
        selected.sort(key=lambda x: x["Config"]["Labels"][KIND] != "engine")
        for container in selected:
            self.verify_owner(container)
            args = ["kill", container["Id"]] if crash else ["stop", "--time", "30", container["Id"]]
            self.docker(*args, timeout=40)
            print("[CERRADO] " + container["Name"].lstrip("/"))
        if not selected:
            print("Ese componente ya esta cerrado.")

    def console(self, entity, part="both", follow=False):
        kinds = {"fo"} if self.role == "fo" else ({"monitor", "engine"} if part == "both" else {part})
        found = [x for x in self.inventory() if x["Config"]["Labels"].get(ENTITY) == safe_id(entity)
                 and x["Config"]["Labels"].get(KIND) in kinds]
        if not found:
            raise LauncherError("No existe ese componente en este equipo.")
        for item in found:
            if follow:
                self.docker("logs", "--tail", "80", item["Id"], capture=False)
            elif item["State"]["Running"]:
                open_console(item, self.home / "consoles", self.windows)
            else:
                print(item["Name"].lstrip("/") + " esta cerrado; usa abrir/recuperar.")

    def status(self):
        self.check_docker()
        for item in self.inventory():
            print(f"{item['Name'].lstrip('/'):45} {item['State']['Status']}")
        try:
            health = self.api("/health")
            print("AWS: conectado | Kafka preparado: " + str(health.get("kafka")))
            for station in self.api("/api/stations"):
                print(f"  {station['id']}: {station['status']}")
        except LauncherError as error:
            print("AWS: " + str(error))

    def compose(self, *args, capture=False):
        env = dict(os.environ, KAFKA_PUBLIC_HOST=self.config["aws_host"],
                   WM_SEED_DEMO="0", WM_IMAGE=self.config["image"], WM_LAUNCHER_NAMESPACE=self.namespace)
        return self.docker("compose", "--project-name", self.config.get("cloud_project", "sd"),
                           "--file", self.source() / "docker-compose.yml", *args,
                           capture=capture, timeout=900, env=env)

    def cloud_up(self):
        if self.role != "cloud":
            raise LauncherError("La nube solo se arranca con el perfil cloud en AWS.")
        self.check_docker()
        self.ensure_image()
        self.compose("up", "--detach", "--no-build", "--wait", "--wait-timeout", "180")

    def clean(self):
        self.check_docker()
        if self.role == "cloud":
            self.compose("down", "--timeout", "30")  # Sin -v: se conserva Kafka y SQLite.
            return
        inventory = self.inventory()
        for item in sorted(inventory, key=lambda x: x["Config"]["Labels"].get(KIND) != "engine"):
            self.verify_owner(item)
            if item["State"]["Running"]:
                self.docker("stop", "--time", "30", item["Id"], timeout=40)
            self.docker("rm", item["Id"])
        result = self.docker("network", "inspect", self.network, check=False)
        if result.returncode == 0:
            info = json.loads(result.stdout)[0]
            if (info.get("Labels") or {}).get(OWNER) == self.namespace and not info.get("Containers"):
                self.docker("network", "rm", self.network)
        print("CLEAN terminado. Se conservan los volumenes y los datos.")
