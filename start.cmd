@echo off
setlocal EnableExtensions
title MSH

cd /d "%~dp0"

set "MSH_FRESH_INSTALL=0"
set "MSH_RESUME_EXISTING=0"
set "MSH_RESUME_EXIT=0"
if "%~1"=="" goto :arguments_ready
if /I "%~1"=="--fresh" (
    if not "%~2"=="" goto :usage_error
    set "MSH_FRESH_INSTALL=1"
    goto :arguments_ready
)
if /I "%~1"=="--resume" (
    if not "%~2"=="" goto :usage_error
    set "MSH_RESUME_EXISTING=1"
    goto :arguments_ready
)
if /I "%~1"=="--help" goto :show_help
if /I "%~1"=="/?" goto :show_help
goto :usage_error

:arguments_ready
if not defined MSH_WEB_BIND set "MSH_WEB_BIND=127.0.0.1"
if not defined COMPOSE_PROJECT_NAME set "COMPOSE_PROJECT_NAME=msh"
set "MSH_WEB_PORT_EXPLICIT=1"
if not defined MSH_WEB_PORT (
    set "MSH_WEB_PORT=5000"
    set "MSH_WEB_PORT_EXPLICIT=0"
)
set "MSH_DATA_DIR_DEFAULTED=0"
if not defined MSH_DATA_DIR (
    for %%I in ("%~dp0data") do set "MSH_DATA_DIR=%%~fI"
    set "MSH_DATA_DIR_DEFAULTED=1"
)
set "MSH_RESULTS_DIR_DEFAULTED=0"
if not defined MSH_RESULTS_DIR (
    for %%I in ("%~dp0results") do set "MSH_RESULTS_DIR=%%~fI"
    set "MSH_RESULTS_DIR_DEFAULTED=1"
)

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

call :resolve_runtime_state
if errorlevel 1 (
    echo.
    echo MSH could not resolve its existing runtime state safely.
    pause
    exit /b 1
)

if "%MSH_FRESH_INSTALL%"=="1" (
    call :reset_device_state
    if errorlevel 1 exit /b 1
)

call :resolve_build_commit
if errorlevel 1 (
    echo.
    echo MSH could not determine an immutable build commit from this checkout.
    pause
    exit /b 1
)

call :start_update_agent
if errorlevel 1 (
    echo.
    echo The MSH host update agent could not be started safely.
    pause
    exit /b 1
)

echo Building the current MSH services from %MSH_BUILD_COMMIT%...
docker compose build relay flask recorder
if errorlevel 1 (
    echo.
    echo MSH images could not be built. Review the Docker error above.
    pause
    exit /b 1
)

echo.
echo Starting the MSH background services...
echo   - Federation relay
echo   - Ollama service
echo   - Managed recorder
docker compose up -d relay ollama recorder
if errorlevel 1 (
    echo.
    echo MSH background services could not be started. Review the Docker error above.
    pause
    exit /b 1
)

call :ensure_ollama_model
if errorlevel 1 (
    echo.
    echo MSH background services remain running, but the webapp will not be opened with a missing benchmark model.
    echo Correct the network or Ollama error above, then run start.cmd again.
    pause
    exit /b 1
)

if "%MSH_RESUME_EXISTING%"=="1" (
    call :run_existing_setup_resume
    set "MSH_RESUME_EXIT=%ERRORLEVEL%"
)

echo Starting the Flask workbench on %MSH_WEB_BIND%:%MSH_WEB_PORT%...
docker compose up -d flask
if errorlevel 1 (
    echo.
    echo The MSH webapp could not be started. Review the Docker error above.
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
set "MSH_WEB_CLIENT_HOST=%MSH_WEB_BIND%"
if "%MSH_WEB_CLIENT_HOST%"=="0.0.0.0" set "MSH_WEB_CLIENT_HOST=127.0.0.1"
set "MSH_BASE_URL=http://%MSH_WEB_CLIENT_HOST%:%MSH_WEB_PORT_RESOLVED%"
set "MSH_ONBOARDING_URL=%MSH_BASE_URL%/onboarding"
set "MSH_OPEN_URL=%MSH_BASE_URL%"
if "%MSH_FRESH_INSTALL%"=="1" (
    set "MSH_ONBOARDING_URL=%MSH_BASE_URL%/onboarding?fresh=1&reset=%RANDOM%%RANDOM%"
    set "MSH_OPEN_URL=%MSH_ONBOARDING_URL%"
)

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

echo Waiting for the MSH webapp...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(90); do { try { $response = Invoke-WebRequest -UseBasicParsing -Uri '%MSH_BASE_URL%/onboarding' -TimeoutSec 2; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 } } catch {}; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1"
if errorlevel 1 (
    echo.
    echo The containers started, but MSH did not become ready.
    echo Recent Flask log:
    docker compose logs --tail 60 flask
    echo.
    echo Recent Federation relay log:
    docker compose logs --tail 40 relay
    pause
    exit /b 1
)

if not "%MSH_RESUME_EXISTING%"=="1" goto :resume_complete
if "%MSH_RESUME_EXIT%"=="0" goto :resume_success
if "%MSH_RESUME_EXIT%"=="4" goto :resume_partial
if "%MSH_RESUME_EXIT%"=="2" goto :resume_missing
goto :resume_failed

:resume_success
set "MSH_OPEN_URL=%MSH_BASE_URL%/federation"
echo Existing identity, Federation membership, saved capability evidence, and contribution intent are ready.
goto :resume_complete

:resume_partial
set "MSH_OPEN_URL=%MSH_BASE_URL%/federation"
echo Existing setup reconnected, but saved capability evidence needs explicit review.
echo Federation will open without rerunning inspection or benchmarks.
goto :resume_complete

:resume_failed
set "MSH_OPEN_URL=%MSH_BASE_URL%/onboarding?repair=1"
echo Existing setup could not be resumed safely.
echo The guided repair page will open. No identity or Federation was replaced.
goto :resume_complete

:resume_missing
set "MSH_OPEN_URL=%MSH_ONBOARDING_URL%"
echo No saved device identity and Federation membership were found.
echo First-time onboarding is required on this machine.
goto :resume_complete

:resume_complete
echo.
echo MSH is running:        %MSH_BASE_URL%
echo Onboarding:            "%MSH_ONBOARDING_URL%"
echo Federation:            %MSH_BASE_URL%/federation
echo Recorder status:       %MSH_BASE_URL%/status
echo Documentation:         %MSH_BASE_URL%/docs
echo Device data:           %MSH_DATA_DIR%
echo Federation state:      %MSH_RELAY_VOLUME_NAME%
echo Running build commit:  %MSH_BUILD_COMMIT%
echo.
echo Web access is limited to this MSH machine by default.
echo To pair another device, open MSH using this machine's LAN or VPN address.
echo Setup, Federation identity, pairing state, recording state, checkpoints,
echo downloaded Ollama models, and recorded data are preserved between normal starts.
echo.

start "" "%MSH_OPEN_URL%"
exit /b 0

:resolve_build_commit
set "MSH_BUILD_COMMIT="
for /f "usebackq delims=" %%C in (`git rev-parse --verify HEAD^^{commit} 2^>nul`) do set "MSH_BUILD_COMMIT=%%C"
if not defined MSH_BUILD_COMMIT exit /b 1
powershell -NoProfile -Command "if ('%MSH_BUILD_COMMIT%' -match '^[0-9a-fA-F]{40}$') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    set "MSH_BUILD_COMMIT="
    exit /b 1
)
for /f "usebackq delims=" %%C in (`powershell -NoProfile -Command "'%MSH_BUILD_COMMIT%'.ToLowerInvariant()"`) do set "MSH_BUILD_COMMIT=%%C"
exit /b 0

:start_update_agent
if not exist "%~dp0scripts\windows\msh_update_agent.ps1" exit /b 1
where powershell >nul 2>&1
if errorlevel 1 exit /b 1
start "MSH Update Agent" /b powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "%~dp0scripts\windows\msh_update_agent.ps1" -RepoRoot "%~dp0" -DataDirectory "%MSH_DATA_DIR%" >nul 2>&1
if errorlevel 1 exit /b 1
exit /b 0

:resolve_runtime_state
set "MSH_RUNTIME_FILE=%TEMP%\msh-runtime-%RANDOM%-%RANDOM%.txt"
if exist "%MSH_RUNTIME_FILE%" del /q "%MSH_RUNTIME_FILE%" >nul 2>&1
if "%MSH_WEB_PORT_EXPLICIT%"=="1" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\resolve_msh_web_port.ps1" -BindAddress "%MSH_WEB_BIND%" -PreferredPort %MSH_WEB_PORT% -OutputFile "%MSH_RUNTIME_FILE%"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\resolve_msh_web_port.ps1" -BindAddress "%MSH_WEB_BIND%" -PreferredPort %MSH_WEB_PORT% -OutputFile "%MSH_RUNTIME_FILE%" -AllowFallback
)
set "MSH_RUNTIME_EXIT=%ERRORLEVEL%"
if not "%MSH_RUNTIME_EXIT%"=="0" (
    echo Runtime-state resolver exited with code %MSH_RUNTIME_EXIT%.
    if exist "%MSH_RUNTIME_FILE%" type "%MSH_RUNTIME_FILE%"
    if exist "%MSH_RUNTIME_FILE%" del /q "%MSH_RUNTIME_FILE%" >nul 2>&1
    exit /b %MSH_RUNTIME_EXIT%
)
if not exist "%MSH_RUNTIME_FILE%" (
    echo Runtime-state resolver did not create its output file.
    exit /b 1
)
for /f "usebackq tokens=1,* delims==" %%A in ("%MSH_RUNTIME_FILE%") do (
    if /I "%%A"=="MSH_WEB_PORT" set "MSH_WEB_PORT=%%B"
    if /I "%%A"=="MSH_RELAY_VOLUME_NAME" set "MSH_RELAY_VOLUME_NAME=%%B"
    if /I "%%A"=="MSH_OLLAMA_VOLUME_NAME" set "MSH_OLLAMA_VOLUME_NAME=%%B"
    if /I "%%A"=="MSH_MODEL_PROVIDER_VOLUME_NAME" set "MSH_MODEL_PROVIDER_VOLUME_NAME=%%B"
    if /I "%%A"=="MSH_DATA_DIR" set "MSH_DATA_DIR=%%B"
    if /I "%%A"=="MSH_RESULTS_DIR" set "MSH_RESULTS_DIR=%%B"
)
if not defined MSH_WEB_PORT echo Runtime-state resolver omitted MSH_WEB_PORT.
if not defined MSH_RELAY_VOLUME_NAME echo Runtime-state resolver omitted MSH_RELAY_VOLUME_NAME.
if not defined MSH_OLLAMA_VOLUME_NAME echo Runtime-state resolver omitted MSH_OLLAMA_VOLUME_NAME.
if not defined MSH_MODEL_PROVIDER_VOLUME_NAME echo Runtime-state resolver omitted MSH_MODEL_PROVIDER_VOLUME_NAME.
if not defined MSH_DATA_DIR echo Runtime-state resolver omitted MSH_DATA_DIR.
if not defined MSH_RESULTS_DIR echo Runtime-state resolver omitted MSH_RESULTS_DIR.
if not defined MSH_WEB_PORT goto :runtime_state_invalid
if not defined MSH_RELAY_VOLUME_NAME goto :runtime_state_invalid
if not defined MSH_OLLAMA_VOLUME_NAME goto :runtime_state_invalid
if not defined MSH_MODEL_PROVIDER_VOLUME_NAME goto :runtime_state_invalid
if not defined MSH_DATA_DIR goto :runtime_state_invalid
if not defined MSH_RESULTS_DIR goto :runtime_state_invalid
if exist "%MSH_RUNTIME_FILE%" del /q "%MSH_RUNTIME_FILE%" >nul 2>&1
echo MSH web port reserved: %MSH_WEB_BIND%:%MSH_WEB_PORT%
echo MSH device data:       %MSH_DATA_DIR%
echo MSH Federation state:  %MSH_RELAY_VOLUME_NAME%
echo.
exit /b 0

:runtime_state_invalid
echo Resolver output was:
type "%MSH_RUNTIME_FILE%"
if exist "%MSH_RUNTIME_FILE%" del /q "%MSH_RUNTIME_FILE%" >nul 2>&1
exit /b 1

:run_existing_setup_resume
echo.
echo Reconnecting the saved Federation before starting the webapp...
echo The resume will reuse saved inspection and benchmark evidence without rerunning either one.
echo Saved contribution intent is left unchanged until the long-running Flask app validates the saved evidence.
rem Ensure only the isolated resume process uses this device identity. The
rem long-running Flask service starts after the read-only resume has completed.
docker compose stop flask >nul 2>&1
docker compose run --rm --no-deps --entrypoint python flask -m catalog.flask_app.services.existing_setup_resume
exit /b %ERRORLEVEL%

:ensure_ollama_model
set "MSH_AI_MODEL_RESOLVED="
for /f "usebackq delims=" %%M in (`docker compose run --rm --no-deps --entrypoint python flask -c "import os; print(os.environ.get('MSH_AI_MODEL') or 'llama3.2:3b')"`) do set "MSH_AI_MODEL_RESOLVED=%%M"
if not defined MSH_AI_MODEL_RESOLVED set "MSH_AI_MODEL_RESOLVED=llama3.2:3b"

echo Ensuring Ollama benchmark model is installed: %MSH_AI_MODEL_RESOLVED%
docker compose exec -T ollama ollama show "%MSH_AI_MODEL_RESOLVED%" >nul 2>&1
if not errorlevel 1 (
    echo Ollama benchmark model is ready.
    echo.
    exit /b 0
)

set "MSH_MODEL_ATTEMPT=1"
:pull_ollama_model
echo Pulling %MSH_AI_MODEL_RESOLVED% ^(attempt %MSH_MODEL_ATTEMPT% of 3^) ...
docker compose --profile model-install run --rm --entrypoint /bin/ollama ollama-pull pull "%MSH_AI_MODEL_RESOLVED%"
set "MSH_MODEL_PULL_EXIT=%ERRORLEVEL%"

docker compose exec -T ollama ollama show "%MSH_AI_MODEL_RESOLVED%" >nul 2>&1
if not errorlevel 1 (
    echo Ollama benchmark model is installed and verified.
    echo.
    exit /b 0
)

if %MSH_MODEL_ATTEMPT% GEQ 3 (
    echo.
    echo ERROR: Ollama does not contain the required model: %MSH_AI_MODEL_RESOLVED%
    if not "%MSH_MODEL_PULL_EXIT%"=="0" echo The final pull command exited with code %MSH_MODEL_PULL_EXIT%.
    echo Installed Ollama models:
    docker compose exec -T ollama ollama list
    exit /b 1
)

set /a MSH_MODEL_ATTEMPT+=1
powershell -NoProfile -Command "Start-Sleep -Seconds 3"
goto :pull_ollama_model

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
echo   start.cmd            Start MSH and preserve all existing state.
echo   start.cmd --resume   Reconnect, reuse saved capability evidence, then start MSH.
echo   start.cmd --fresh    Reset device/Federation setup, verify it, then start MSH.
echo.
echo Normal and resume modes preserve identity, Federation membership, recordings,
echo source configuration, recorder checkpoints, results, and downloaded models.
echo Resume mode never runs inspection or benchmarks and never replaces Federation authority.
echo All modes install and verify the exact configured Ollama benchmark model.
echo The supported launcher also keeps a bounded local host update agent running.
echo The --fresh option requires typing RESET and preserves recordings,
echo source configuration, recorder checkpoints, results, and Ollama models.
exit /b 0

:usage_error
echo Unknown option: %~1
echo Run start.cmd --help for supported options.
exit /b 2