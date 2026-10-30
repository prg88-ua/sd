@echo off
setlocal
cd /d "%~dp0"
if exist "dist\WaterManagement.exe" (
  "dist\WaterManagement.exe" %*
) else (
  python wm.py %*
)
if errorlevel 1 pause
