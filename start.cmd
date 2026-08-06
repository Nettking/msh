@echo off
setlocal
title MSH

cd /d "%~dp0"

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
