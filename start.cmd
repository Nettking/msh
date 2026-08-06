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

set "MSH_WEB_PORT_RESOLVED="
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$lines = @(docker compose port flask 5000); if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { exit 1 }; $binding = [string]$lines[0]; if ($binding -match ':(\d+)$') { $Matches[1] } else { exit 1 }"`) do set "MSH_WEB_PORT_RESOLVED=%%P"
if not defined MSH_WEB_PORT_RESOLVED (
    echo Could not determine the published Flask port.
    docker compose ps flask
    pause
    exit /b 1
)

set "MSH_BASE_URL=http://localhost:%MSH_WEB_PORT_RESOLVED%"
set "MSH_ONBOARDING_URL=%MSH_BASE_URL%/onboarding"
if "%MSH_FRESH_INSTALL%"=="1" set "MSH_ONBOARDING_URL=%MSH_BASE_URL%/onboarding?fresh=1&reset=%RANDOM%%RANDOM%"

if "%MSH_FRESH_INSTALL%"=="1" (
    echo Verifying fresh state inside the started Flask container...
    docker compose exec -T flask python -m catalog.flask_app.services.device_state_reset --verify-fresh
    if errorlevel 1 (
        echo.
        echo MSH started, but its authoritative setup state is not fresh.
        echo The browser will not be opened because old identity or Federation state remains.
        echo Review the specific verification failure above.
        pause
        exit /b 1
    )
)

echo Waiting for capability-first onboarding...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(90); do { try { $response = Invoke-WebRequest -UseBasicParsing -Uri '%MSH_BASE_URL%/onboarding' -TimeoutSec 2; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 } } catch {}; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1"
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

echo MSH is running:        %MSH_BASE_URL%
echo Onboarding:            %MSH_ONBOARDING_URL%
echo Federation:            %MSH_BASE_URL%/federation
echo Recorder status:       %MSH_BASE_URL%/status
echo Documentation:         %MSH_BASE_URL%/docs
echo.
echo Web access is limited to this MSH machine by default.
echo To pair another device, open MSH using this machine's LAN or VPN address.
echo Setup, Federation identity, pairing state, recording state, checkpoints,
echo downloaded Ollama models, and recorded data are preserved between normal starts.
echo.

start "" "%MSH_ONBOARDING_URL%"
exit /b 0

:reset_device_state
echo.
echo FRESH DEVICE INSTALL
echo This permanently removes this checkout's:
echo   - MSH device identity and keys
echo   - Federation membership, pairing, onboarding, inspection, and benchmark state
echo   - current and retained legacy Federation state layouts
echo   - all local Federation relay authority state
echo   - all saved server role and device setup choices
echo   - saved browser onboarding-step progress on the page opened afterward
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

echo Resolving and clearing current plus retained state paths...
docker compose run --rm --no-deps --build --entrypoint python flask -m catalog.flask_app.services.device_state_reset
if errorlevel 1 (
    echo.
    echo Fresh device reset did not complete. Review the specific path error above.
    echo Recorded data, recorder checkpoints, results, and Ollama models were not targeted.
    pause
    exit /b 1
)

echo Fresh device reset completed. MSH will now start and verify empty state.
echo.
exit /b 0

:show_help
echo Usage:
echo   start.cmd           Start MSH and preserve all existing state.
echo   start.cmd --fresh   Reset device/Federation setup, verify it, then start MSH.
echo.
echo The --fresh option requires typing RESET and preserves recordings,
echo source configuration, recorder checkpoints, results, and Ollama models.
exit /b 0

:usage_error
echo Unknown option: %~1
echo Run start.cmd --help for supported options.
exit /b 2
