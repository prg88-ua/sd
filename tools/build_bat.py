"""Genera un BAT con instrucciones de lanzamiento, sin empaquetar la aplicación."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build():
    launcher = (ROOT / "tools/launcher.ps1").read_text(encoding="utf-8")
    # El .bat solo usa cmd para abrir PowerShell. Todos los datos se leen del
    # propio archivo; nunca se interpolan rutas del usuario como comandos.
    header = r'''@echo off
setlocal
set "WM_BAT_PATH=%~f0"
set "WM_BAT_ACTION=%~1"
set "WM_BAT_SOURCE=%~2"
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$t=[IO.File]::ReadAllText($env:WM_BAT_PATH); $m=[regex]::Match($t,'(?s)\r?\n#<WM_POWERSHELL>\r?\n(.*?)\r?\n#<WM_END>\s*$'); if(!$m.Success){Write-Host 'BAT incompleto';exit 1}; & ([ScriptBlock]::Create($m.Groups[1].Value))"
exit /b %errorlevel%
#<WM_POWERSHELL>
'''
    content = header + launcher.rstrip() + "\n#<WM_END>\n"
    output = ROOT / "WaterManagement.bat"
    output.write_bytes(content.replace("\r\n", "\n").replace("\n", "\r\n").encode("utf-8"))
    print(f"Creado {output.name}: {output.stat().st_size} bytes; solo instrucciones, sin aplicación empaquetada")
    return output


if __name__ == "__main__":
    build()
