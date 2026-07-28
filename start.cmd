@echo off
setlocal
title MSH Recorder Station

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

echo Starting the MSH app and recorder service...
docker compose up -d --build flask recorder
if errorlevel 1 (
    echo.
    echo MSH could not be started. Review the Docker error above.
    pause
    exit /b 1
)

echo.
docker compose ps flask recorder
echo.

echo Waiting for the MSH web page...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(90); do { try { $response = Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:5000/startup' -TimeoutSec 2; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 } } catch {}; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1"
if errorlevel 1 (
    echo.
    echo The containers started, but the MSH web page did not become ready.
    echo Recent app log:
    docker compose logs --tail 40 flask
    pause
    exit /b 1
)

echo MSH is running:        http://localhost:5000
echo Setup:                 http://localhost:5000/startup
echo Recorder status:      http://localhost:5000/status
echo Web access is limited to this MSH machine by default.
echo.
echo Setup, recording state, checkpoints, and recorded data remain under data\
echo and will be reused the next time start.cmd is run.

start "" "http://localhost:5000/startup"
exit /b 0
