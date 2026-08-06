@echo off
setlocal EnableExtensions
title MSH

cd /d "%~dp0"

set "MSH_FRESH_INSTALL=0"
if "%~1"=="" goto :arguments_ready
if /I "%~1"=="--fresh" (
    if not "%~2"=="" goto :usage_error
    set "MSH_FRESH_INSTALL=1"
    goto :arguments_ready
)
if /I "%~1"=="--help" goto :show_help
if /I "%~1"=="/?" goto :show_help
goto :usage_error

:arguments_ready
if not defined MSH_WEB_BIND set "MSH_WEB_BIND=127.0.0.1"

where docker >nul 2>&1
if errorlevel 1 (
    echo Docker was not found.
    echo Install Docker Desktop, then run start.cmd again.
    pause
    exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
    echo Docker is not running.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

if "%MSH_FRESH_INSTALL%"=="1" (
    call :reset_device_state
    if errorlevel 1 exit /b 1
)

echo Starting the current MSH core services...
echo   - Federation relay
echo   - Ollama service
echo   - Flask workbench
echo   - Managed recorder
docker compose up -d --build relay ollama flask recorder
if errorlevel 1 (
    echo.
    echo MSH could not be started. Review the Docker error above.
    pause
    exit /b 1
)

echo.
docker compose ps relay ollama flask recorder
echo.

echo Waiting for capability-first onboarding...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(90); do { try { $response = Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:5000/onboarding' -TimeoutSec 2; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 } } catch {}; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1"
if errorlevel 1 (
    echo.
    echo The containers started, but MSH onboarding did not become ready.
    echo Recent Flask log:
    docker compose logs --tail 60 flask
    echo.
    echo Recent Federation relay log:
    docker compose logs --tail 40 relay
    pause
    exit /b 1
)

echo MSH is running:        http://localhost:5000
echo Onboarding:            http://localhost:5000/onboarding
echo Federation:            http://localhost:5000/federation
echo Recorder status:       http://localhost:5000/status
echo Documentation:         http://localhost:5000/docs
echo Federation relay:      ws://localhost:8765
echo.
echo Web access is limited to this MSH machine by default.
echo To pair another device, open MSH using this machine's LAN or VPN address.
echo Setup, Federation identity, pairing state, recording state, checkpoints,
echo downloaded Ollama models, and recorded data are preserved between starts.
echo.

start "" "http://localhost:5000/onboarding"
exit /b 0

:reset_device_state
echo.
echo FRESH DEVICE INSTALL
echo This permanently removes this checkout's:
echo   - MSH device identity and keys
echo   - Federation membership, pairing, onboarding, and benchmark state
echo   - local Federation relay authority database
echo   - saved server role and device setup choices
echo.
echo It preserves recorded telemetry, source configuration, recorder checkpoints,
echo analysis results, Docker images, and downloaded Ollama models.
echo.
set "MSH_RESET_CONFIRM="
set /p "MSH_RESET_CONFIRM=Type RESET to continue: "
if /I not "%MSH_RESET_CONFIRM%"=="RESET" (
    echo Fresh install cancelled. No state was removed.
    exit /b 2
)

echo.
echo Stopping MSH before resetting device and Federation state...
docker compose down --remove-orphans
if errorlevel 1 (
    echo MSH containers could not be stopped safely. Nothing else was removed.
    pause
    exit /b 1
)

echo Clearing only the local Federation relay authority volume...
docker compose run --rm --no-deps --build --entrypoint python relay -c "from pathlib import Path; import shutil; root = Path('/var/lib/msh-relay'); [(shutil.rmtree(path) if path.is_dir() and not path.is_symlink() else path.unlink()) for path in tuple(root.iterdir())]"
if errorlevel 1 (
    echo.
    echo The Federation relay state could not be cleared.
    echo Recorded data and Ollama models were not removed.
    pause
    exit /b 1
)

echo Removing host-side device, onboarding, and setup state...
if exist "data\federation" rmdir /s /q "data\federation"
if exist "data\federation" (
    echo.
    echo Could not remove data\federation. Check file permissions or open programs.
    pause
    exit /b 1
)

if exist "data\server_setup\server_settings.json" del /f /q "data\server_setup\server_settings.json"
if exist "data\server_setup\server_settings.json" (
    echo.
    echo Could not remove data\server_setup\server_settings.json.
    echo Check file permissions or open programs.
    pause
    exit /b 1
)

echo Fresh device reset completed. MSH will now start with Identity and Federation empty.
echo.
exit /b 0

:show_help
echo Usage:
echo   start.cmd           Start MSH and preserve all existing state.
echo   start.cmd --fresh   Reset device/Federation setup, then start MSH.
echo.
echo The --fresh option requires typing RESET and preserves recordings,
echo source configuration, recorder checkpoints, results, and Ollama models.
exit /b 0

:usage_error
echo Unknown option: %~1
echo Run start.cmd --help for supported options.
exit /b 2
