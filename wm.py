"""Water Management: menu de laboratorio y linea de comandos equivalente."""
import argparse
import json
import os
from pathlib import Path
import sys

from wm_launcher.runtime import (LauncherError, Runner, default_home, operation_lock,
                                 quantity, read_config, save_json, validate_config)
from wm_launcher.windows import console_worker


def parser():
    result = argparse.ArgumentParser(description="Water Management: AWS + PC de FO + PC de estaciones")
    result.add_argument("--role", choices=("cloud", "fo", "ws"), help="Perfil de ESTE equipo")
    result.add_argument("--aws-host", help="IP publica/DNS de AWS, sin http://")
    result.add_argument("--state-dir", type=Path, default=default_home(), help="Carpeta privada del lanzador")
    result.add_argument("--no-windows", action="store_true", help="No abrir ventanas (automatizacion/Linux)")
    commands = result.add_subparsers(dest="action")
    commands.add_parser("menu", help="Menu interactivo; opcion predeterminada")
    config = commands.add_parser("configure", help="Guardar perfil y direccion de AWS")
    config.add_argument("--central-port", type=int, default=6000)
    config.add_argument("--api-port", type=int, default=5000)
    config.add_argument("--kafka-port", type=int, default=9092)
    config.add_argument("--namespace", default="wm-lab")
    config.add_argument("--cloud-project", default="sd", help="Nombre Compose existente en AWS")
    commands.add_parser("build", help="Preparar/reconstruir la imagen Docker")
    commands.add_parser("up", help="Arrancar la parte cloud en AWS")
    commands.add_parser("status", help="Contenedores locales y estados de AWS")
    commands.add_parser("clean", help="Cerrar componentes y ventanas locales, conservando datos")
    register = commands.add_parser("register", help="Preparar catalogo en AWS sin arrancar clientes")
    register.add_argument("--fo", type=int, default=0)
    register.add_argument("--ws", type=int, default=0)
    for action in ("add", "set"):
        command = commands.add_parser(action)
        command.add_argument("--count", type=int, required=True)
        command.add_argument("--file", help="Archivo de peticiones para FO (opcional)")
    for action in ("start", "stop", "crash", "console", "logs"):
        command = commands.add_parser(action)
        command.add_argument("--id", required=True)
        command.add_argument("--part", choices=("both", "monitor", "engine"), default="both")
        if action == "start":
            command.add_argument("--file", help="Archivo de peticiones para FO (opcional)")
    return result


def configure(home, config, ask=True):
    if ask:
        print("\nCada equipo tiene su papel. AWS ya debe estar desplegado antes de la evaluacion.")
        selected = input(f"Perfil [fo/ws/cloud] [{config['role'] or 'fo'}]: ").strip()
        config["role"] = selected or config["role"] or "fo"
        config["aws_host"] = input(f"IP publica o DNS de AWS [{config['aws_host']}]: ").strip() or config["aws_host"]
        if config["role"] == "cloud":
            config["cloud_project"] = input(f"Proyecto Compose existente [{config.get('cloud_project', 'sd')}]: ").strip() or config.get("cloud_project", "sd")
    validate_config(config)
    save_json(home / "config.json", config)
    print("Configuracion guardada.")
    return config


def count_input(prompt):
    try:
        return quantity(int(input(prompt).strip()))
    except ValueError:
        raise LauncherError("Introduce una cantidad entera entre 0 y 1000.") from None


def part_input(role):
    if role == "fo":
        return "both"
    choice = input("Componente [both/monitor/engine] [both]: ").strip() or "both"
    if choice not in ("both", "monitor", "engine"):
        raise LauncherError("Elige both, monitor o engine.")
    return choice


def execute(runner, action, args):
    # El fichero de bloqueo no es inventario: los contenedores etiquetados son la
    # verdad. Por eso reabrir el lanzador tras un fallo no pierde los componentes.
    with operation_lock(runner.home):
        if action == "status":
            runner.status()
        elif action == "build":
            runner.check_docker()
            runner.ensure_image(force=True)
        elif action == "up":
            runner.cloud_up()
        elif action == "register":
            fo_count, ws_count = quantity(args.fo), quantity(args.ws)
            response = runner.register([f"FO-{n:02d}" for n in range(1, fo_count + 1)],
                                       [f"WS-{n:02d}" for n in range(1, ws_count + 1)])
            print("Catalogo preparado: " + json.dumps(response["created"]))
        elif action == "add":
            runner.check_docker()
            runner.start_ids(runner.next_ids(quantity(args.count)), batch_file=args.file)
        elif action == "set":
            runner.check_docker()
            runner.set_count(quantity(args.count), batch_file=args.file)
        elif action == "start":
            runner.start_ids([args.id], part=args.part, batch_file=args.file)
        elif action in ("stop", "crash"):
            runner.check_docker()
            runner.stop_id(args.id, part=args.part, crash=action == "crash")
        elif action in ("console", "logs"):
            runner.check_docker()
            runner.console(args.id, part=args.part, follow=action == "logs")
        elif action == "clean":
            runner.clean()


def menu(runner):
    from types import SimpleNamespace
    while True:
        noun = "operarios" if runner.role == "fo" else "estaciones"
        print("\n" + "=" * 60)
        print(" WATER MANAGEMENT | " + runner.role.upper() + " | AWS: " + runner.config["aws_host"])
        print("=" * 60)
        if runner.role == "cloud":
            print("1. Arrancar servicios de AWS\n2. Preparar catalogo de IDs")
        else:
            print(f"1. Abrir mas {noun}\n2. Ajustar el total de {noun}")
        print("3. Abrir/recuperar por ID\n4. Abrir terminales de un ID\n5. Ver estado y mensajes")
        print("6. Cerrar por ID\n7. Simular caida abrupta\n8. CLEAN - cerrar todo lo local")
        print("9. Preparar/reconstruir imagen\n0. Salir del lanzador")
        try:
            choice = input("Selecciona una opcion: ").strip()
            arguments = SimpleNamespace(file=None, part="both")
            action = None
            if choice == "0":
                if input("Cerrar componentes locales antes de salir? [s/N]: ").strip().lower() == "s":
                    execute(runner, "clean", arguments)
                return
            if choice in ("1", "2") and runner.role == "cloud":
                if choice == "1":
                    action = "up"
                else:
                    arguments.fo = count_input("Numero de operarios a registrar: ")
                    arguments.ws = count_input("Numero de estaciones a registrar: ")
                    action = "register"
            elif choice in ("1", "2"):
                arguments.count = count_input("Cantidad: ")
                if runner.role == "fo" and arguments.count:
                    arguments.file = input("Archivo para ejecutar al abrir (Enter: menu manual): ").strip() or None
                action = "add" if choice == "1" else "set"
            elif choice in ("3", "4", "6", "7"):
                if runner.role == "cloud":
                    raise LauncherError("Estas opciones se usan en los PCs del laboratorio.")
                arguments.id = input("ID: ").strip()
                arguments.part = part_input(runner.role)
                action = {"3": "start", "4": "console", "6": "stop", "7": "crash"}[choice]
            elif choice == "5":
                action = "status"
            elif choice == "8":
                if input("Cerrar todos los componentes de ESTE perfil/equipo? [s/N]: ").strip().lower() == "s":
                    action = "clean"
            elif choice == "9":
                action = "build"
            else:
                print("Opcion no valida.")
            if action:
                execute(runner, action, arguments)
        except LauncherError as error:
            print("\n[AVISO] " + str(error))
        except KeyboardInterrupt:
            print("\nOperacion interrumpida. Los componentes ya abiertos siguen disponibles.")
        except EOFError:
            return


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "_console":
        internal = argparse.ArgumentParser()
        internal.add_argument("_console")
        internal.add_argument("--container", required=True)
        return console_worker(internal.parse_args(argv).container)
    args = parser().parse_args(argv)
    try:
        config = read_config(args.state_dir)
        for key in ("role", "aws_host"):
            if getattr(args, key):
                config[key] = getattr(args, key)
        action = args.action or "menu"
        if action == "configure":
            for key in ("central_port", "api_port", "kafka_port", "namespace", "cloud_project"):
                config[key] = getattr(args, key)
            configure(args.state_dir, config, ask=not (args.role and args.aws_host))
            return 0
        if action == "menu" and (not config["role"] or not config["aws_host"]):
            configure(args.state_dir, config)
        runner = Runner(config, args.state_dir, windows=not args.no_windows)
        if action == "menu":
            menu(runner)
        else:
            execute(runner, action, args)
        return 0
    except (LauncherError, ValueError, OSError) as error:
        if os.environ.get("WM_LAUNCHER_DEBUG") == "1":
            # Diagnóstico opcional del ejecutable; el menú normal conserva mensajes breves.
            import traceback
            traceback.print_exc()
        print("[ERROR] " + str(error), file=sys.stderr)
        return 1
    except (KeyboardInterrupt, EOFError):
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
