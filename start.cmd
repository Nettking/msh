@echo off
setlocal EnableExtensions
title FCP

cd /d "%~dp0"

set "FCP_FRESH_INSTALL=0"
set "FCP_RESUME_EXISTING=0"
set "FCP_RESUME_EXIT=0"
if "%~1"=="" goto :arguments_ready
if /I "%~1"=="--fresh" (
    if not "%~2"=="" goto :usage_error
    set "FCP_FRESH_INSTALL=1"
    goto :arguments_ready
)
if /I "%~1"=="--resume" (
    if not "%~2"=="" goto :usage_error
    set "FCP_RESUME_EXISTING=1"
    goto :arguments_ready
)
if /I "%~1"=="--help" goto :show_help
if /I "%~1"=="/?" goto :show_help
goto :usage_error

:arguments_ready
if not defined FCP_WEB_BIND set "FCP_WEB_BIND=127.0.0.1"
if not defined FCP_RELAY_BIND set "FCP_RELAY_BIND=127.0.0.1"
if not defined COMPOSE_PROJECT_NAME set "COMPOSE_PROJECT_NAME=fcp"
set "FCP_WEB_PORT_EXPLICIT=1"
if not defined FCP_WEB_PORT (
    set "FCP_WEB_PORT=5000"
    set "FCP_WEB_PORT_EXPLICIT=0"
)
set "FCP_DATA_DIR_DEFAULTED=0"
if not defined FCP_DATA_DIR (
    for %%I in ("%~dp0data") do set "FCP_DATA_DIR=%%~fI"
    set "FCP_DATA_DIR_DEFAULTED=1"
)
set "FCP_RESULTS_DIR_DEFAULTED=0"
if not defined FCP_RESULTS_DIR (
    for %%I in ("%~dp0results") do set "FCP_RESULTS_DIR=%%~fI"
    set "FCP_RESULTS_DIR_DEFAULTED=1"
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

rem A previous interrupted --fresh from an older build may have removed these
rem four Git-tracked runtime-root scaffolding files. They are immutable checkout
rem content, not FCP application state. Restore only missing canonical copies.
call :repair_checkout_scaffolding
if errorlevel 1 (
    echo.
    echo FCP could not restore immutable checkout scaffolding safely.
    pause
    exit /b 1
)

call :resolve_runtime_state
if errorlevel 1 (
    echo.
    echo FCP could not resolve its existing runtime state safely.
    pause
    exit /b 1
)

if "%FCP_FRESH_INSTALL%"=="1" (
    call :reset_device_state
    if errorlevel 1 exit /b 1
    rem Current reset code preserves Git-tracked root scaffolding directly.
    rem Keep this repair as backward-compatible protection for older damaged checkouts.
    call :repair_checkout_scaffolding
    if errorlevel 1 (
        echo.
        echo Fresh reset completed, but immutable checkout scaffolding could not be restored.
        pause
        exit /b 1
    )
)

call :resolve_build_commit
if errorlevel 1 (
    echo.
    echo FCP could not determine an immutable build commit from this checkout.
    pause
    exit /b 1
)

call :start_update_agent
if errorlevel 1 (
    echo.
    echo The FCP host update agent could not be started safely.
    pause
    exit /b 1
)

echo Building the current FCP services from %FCP_BUILD_COMMIT%...
docker compose build relay flask recorder
if errorlevel 1 (
    echo.
    echo FCP images could not be built. Review the Docker error above.
    pause
    exit /b 1
)

echo.
echo Starting the FCP background services...
echo   - Federation relay
echo   - Ollama service
echo   - Managed recorder
docker compose up -d relay ollama recorder
if errorlevel 1 (
    echo.
    echo FCP background services could not be started. Review the Docker error above.
    pause
    exit /b 1
)

call :ensure_ollama_model
if errorlevel 1 (
    echo.
    echo FCP background services remain running, but the webapp will not be opened with a missing benchmark model.
    echo Correct the network or Ollama error above, then run start.cmd again.
    pause
    exit /b 1
)

if "%FCP_RESUME_EXISTING%"=="1" (
    call :run_existing_setup_resume
    set "FCP_RESUME_EXIT=%ERRORLEVEL%"
)

echo Starting the Flask workbench on %FCP_WEB_BIND%:%FCP_WEB_PORT%...
docker compose up -d flask
if errorlevel 1 (
    echo.
    echo The FCP webapp could not be started. Review the Docker error above.
    pause
    exit /b 1
)

echo.
docker compose ps relay ollama flask recorder
echo.

set "FCP_WEB_PORT_RESOLVED="
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$lines = @(docker compose port flask 5000); if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { exit 1 }; $binding = [string]$lines[0]; if ($binding -match ':(\d+)$') { $Matches[1] } else { exit 1 }"`) do set "FCP_WEB_PORT_RESOLVED=%%P"
if not defined FCP_WEB_PORT_RESOLVED (
    echo Could not determine the published Flask port.
    docker compose ps flask
    pause
    exit /b 1
)
set "FCP_WEB_CLIENT_HOST=%FCP_WEB_BIND%"
if "%FCP_WEB_CLIENT_HOST%"=="0.0.0.0" set "FCP_WEB_CLIENT_HOST=127.0.0.1"
set "FCP_BASE_URL=http://%FCP_WEB_CLIENT_HOST%:%FCP_WEB_PORT_RESOLVED%"
set "FCP_ONBOARDING_URL=%FCP_BASE_URL%/onboarding"
set "FCP_OPEN_URL=%FCP_BASE_URL%"
if "%FCP_FRESH_INSTALL%"=="1" (
    set "FCP_ONBOARDING_URL=%FCP_BASE_URL%/onboarding?fresh=1&reset=%RANDOM%%RANDOM%"
    set "FCP_OPEN_URL=%FCP_ONBOARDING_URL%"
)

echo Waiting for the FCP webapp...
powershell -NoProfile -Command "$deadline = (Get-Date).AddSeconds(90); do { try { $response = Invoke-WebRequest -UseBasicParsing -Uri '%FCP_BASE_URL%/onboarding' -TimeoutSec 2; if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { exit 0 } } catch {}; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline); exit 1"
if errorlevel 1 (
    echo.
    echo The containers started, but FCP did not become ready.
    echo Recent Flask log:
    docker compose logs --tail 60 flask
    echo.
    echo Recent Federation relay log:
    docker compose logs --tail 40 relay
    pause
    exit /b 1
)

if not "%FCP_RESUME_EXISTING%"=="1" goto :resume_complete
if "%FCP_RESUME_EXIT%"=="0" goto :resume_success
if "%FCP_RESUME_EXIT%"=="4" goto :resume_partial
if "%FCP_RESUME_EXIT%"=="2" goto :resume_missing
goto :resume_failed

:resume_success
set "FCP_OPEN_URL=%FCP_BASE_URL%/federation"
echo Existing identity, Federation membership, saved capability evidence, and contribution intent are ready.
goto :resume_complete

:resume_partial
set "FCP_OPEN_URL=%FCP_BASE_URL%/federation"
echo Existing setup reconnected, but saved capability evidence needs explicit review.
echo Federation will open without rerunning inspection or benchmarks.
goto :resume_complete

:resume_failed
set "FCP_OPEN_URL=%FCP_BASE_URL%/onboarding?repair=1"
echo Existing setup could not be resumed safely.
echo The guided repair page will open. No identity or Federation was replaced.
goto :resume_complete

:resume_missing
set "FCP_OPEN_URL=%FCP_ONBOARDING_URL%"
echo No saved device identity and Federation membership were found.
echo First-time onboarding is required on this machine.
goto :resume_complete

:resume_complete
echo.
echo FCP is running:        %FCP_BASE_URL%
echo Onboarding:            "%FCP_ONBOARDING_URL%"
echo Federation:            %FCP_BASE_URL%/federation
echo Recorder status:       %FCP_BASE_URL%/status
echo Documentation:         %FCP_BASE_URL%/docs
echo Device data:           %FCP_DATA_DIR%
echo Federation state:      %FCP_RELAY_VOLUME_NAME%
echo Running build commit:  %FCP_BUILD_COMMIT%
echo.
echo Web and Federation relay access are limited to this FCP machine by default.
echo For Tailscale pairing, use start-tailscale.cmd.
echo For a trusted LAN, explicitly set both FCP_WEB_BIND and FCP_RELAY_BIND
echo to reachable private interfaces before start.cmd, then open FCP through that LAN address.
echo Setup, Federation identity, pairing state, recording state, checkpoints,
echo downloaded Ollama models, and recorded data are preserved between normal starts.
echo.

start "" "%FCP_OPEN_URL%"
exit /b 0

:repair_checkout_scaffolding
for %%F in ("data/.gitkeep" "data/README.md" "results/.gitkeep" "results/README.md") do (
    if not exist "%~dp0%%~F" (
        git cat-file -e "HEAD:%%~F" >nul 2>&1
        if errorlevel 1 (
            echo Immutable checkout scaffolding is missing from HEAD: %%~F
            exit /b 1
        )
        git restore --source=HEAD --worktree -- "%%~F" >nul 2>&1
        if errorlevel 1 (
            echo Could not restore immutable checkout scaffolding: %%~F
            exit /b 1
        )
    )
)
exit /b 0

:resolve_build_commit
set "FCP_BUILD_COMMIT="
for /f "usebackq delims=" %%C in (`git rev-parse --verify HEAD 2^>nul`) do set "FCP_BUILD_COMMIT=%%C"
if not defined FCP_BUILD_COMMIT exit /b 1
powershell -NoProfile -Command "if ('%FCP_BUILD_COMMIT%' -match '^[0-9a-fA-F]{40}$') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    set "FCP_BUILD_COMMIT="
    exit /b 1
)
powershell -NoProfile -Command "$status = @(git status --porcelain=v1 --untracked-files=all 2>$null); if ($LASTEXITCODE -eq 0 -and $status.Count -eq 0) { exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo FCP refuses to label a build from a checkout with local changes.
    set "FCP_BUILD_COMMIT="
    exit /b 1
)
for /f "usebackq delims=" %%C in (`powershell -NoProfile -Command "'%FCP_BUILD_COMMIT%'.ToLowerInvariant()"`) do set "FCP_BUILD_COMMIT=%%C"
exit /b 0

:start_update_agent
if not exist "%~dp0scripts\windows\fcp_update_agent.ps1" exit /b 1
where powershell >nul 2>&1
if errorlevel 1 exit /b 1
start "FCP Update Agent" /b powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "%~dp0scripts\windows\fcp_update_agent.ps1" -RepoRoot "%~dp0" -DataDirectory "%FCP_DATA_DIR%" >nul 2>&1
if errorlevel 1 exit /b 1
exit /b 0

:resolve_runtime_state
set "FCP_RUNTIME_FILE=%TEMP%\fcp-runtime-%RANDOM%-%RANDOM%.txt"
if exist "%FCP_RUNTIME_FILE%" del /q "%FCP_RUNTIME_FILE%" >nul 2>&1
if "%FCP_WEB_PORT_EXPLICIT%"=="1" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\resolve_fcp_web_port.ps1" -BindAddress "%FCP_WEB_BIND%" -PreferredPort %FCP_WEB_PORT% -OutputFile "%FCP_RUNTIME_FILE%"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\resolve_fcp_web_port.ps1" -BindAddress "%FCP_WEB_BIND%" -PreferredPort %FCP_WEB_PORT% -OutputFile "%FCP_RUNTIME_FILE%" -AllowFallback
)
set "FCP_RUNTIME_EXIT=%ERRORLEVEL%"
if not "%FCP_RUNTIME_EXIT%"=="0" (
    echo Runtime-state resolver exited with code %FCP_RUNTIME_EXIT%.
    if exist "%FCP_RUNTIME_FILE%" type "%FCP_RUNTIME_FILE%"
    if exist "%FCP_RUNTIME_FILE%" del /q "%FCP_RUNTIME_FILE%" >nul 2>&1
    exit /b %FCP_RUNTIME_EXIT%
)
if not exist "%FCP_RUNTIME_FILE%" (
    echo Runtime-state resolver did not create its output file.
    exit /b 1
)
for /f "usebackq tokens=1,* delims==" %%A in ("%FCP_RUNTIME_FILE%") do (
    if /I "%%A"=="FCP_WEB_PORT" set "FCP_WEB_PORT=%%B"
    if /I "%%A"=="FCP_RELAY_VOLUME_NAME" set "FCP_RELAY_VOLUME_NAME=%%B"
    if /I "%%A"=="FCP_OLLAMA_VOLUME_NAME" set "FCP_OLLAMA_VOLUME_NAME=%%B"
    if /I "%%A"=="FCP_MODEL_PROVIDER_VOLUME_NAME" set "FCP_MODEL_PROVIDER_VOLUME_NAME=%%B"
    if /I "%%A"=="FCP_DATA_DIR" set "FCP_DATA_DIR=%%B"
    if /I "%%A"=="FCP_RESULTS_DIR" set "FCP_RESULTS_DIR=%%B"
)
if not defined FCP_WEB_PORT echo Runtime-state resolver omitted FCP_WEB_PORT.
if not defined FCP_RELAY_VOLUME_NAME echo Runtime-state resolver omitted FCP_RELAY_VOLUME_NAME.
if not defined FCP_OLLAMA_VOLUME_NAME echo Runtime-state resolver omitted FCP_OLLAMA_VOLUME_NAME.
if not defined FCP_MODEL_PROVIDER_VOLUME_NAME echo Runtime-state resolver omitted FCP_MODEL_PROVIDER_VOLUME_NAME.
if not defined FCP_DATA_DIR echo Runtime-state resolver omitted FCP_DATA_DIR.
if not defined FCP_RESULTS_DIR echo Runtime-state resolver omitted FCP_RESULTS_DIR.
if not defined FCP_WEB_PORT goto :runtime_state_invalid
if not defined FCP_RELAY_VOLUME_NAME goto :runtime_state_invalid
if not defined FCP_OLLAMA_VOLUME_NAME goto :runtime_state_invalid
if not defined FCP_MODEL_PROVIDER_VOLUME_NAME goto :runtime_state_invalid
if not defined FCP_DATA_DIR goto :runtime_state_invalid
if not defined FCP_RESULTS_DIR goto :runtime_state_invalid
if exist "%FCP_RUNTIME_FILE%" del /q "%FCP_RUNTIME_FILE%" >nul 2>&1
echo FCP web port reserved: %FCP_WEB_BIND%:%FCP_WEB_PORT%
echo FCP device data:       %FCP_DATA_DIR%
echo FCP Federation state:  %FCP_RELAY_VOLUME_NAME%
echo.
exit /b 0

:runtime_state_invalid
echo Resolver output was:
type "%FCP_RUNTIME_FILE%"
if exist "%FCP_RUNTIME_FILE%" del /q "%FCP_RUNTIME_FILE%" >nul 2>&1
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
set "FCP_AI_MODEL_RESOLVED="
for /f "usebackq delims=" %%M in (`docker compose run --rm --no-deps --entrypoint python flask -c "import os; print(os.environ.get('FCP_AI_MODEL') or 'llama3.2:3b')"`) do set "FCP_AI_MODEL_RESOLVED=%%M"
if not defined FCP_AI_MODEL_RESOLVED set "FCP_AI_MODEL_RESOLVED=llama3.2:3b"

echo Ensuring Ollama benchmark model is installed: %FCP_AI_MODEL_RESOLVED%
docker compose exec -T ollama ollama show "%FCP_AI_MODEL_RESOLVED%" >nul 2>&1
if not errorlevel 1 (
    echo Ollama benchmark model is ready.
    echo.
    exit /b 0
)

set "FCP_MODEL_ATTEMPT=1"
:pull_ollama_model
echo Pulling %FCP_AI_MODEL_RESOLVED% ^(attempt %FCP_MODEL_ATTEMPT% of 3^) ...
docker compose --profile model-install run --rm --entrypoint /bin/ollama ollama-pull pull "%FCP_AI_MODEL_RESOLVED%"
set "FCP_MODEL_PULL_EXIT=%ERRORLEVEL%"

docker compose exec -T ollama ollama show "%FCP_AI_MODEL_RESOLVED%" >nul 2>&1
if not errorlevel 1 (
    echo Ollama benchmark model is installed and verified.
    echo.
    exit /b 0
)

if %FCP_MODEL_ATTEMPT% GEQ 3 (
    echo.
    echo ERROR: Ollama does not contain the required model: %FCP_AI_MODEL_RESOLVED%
    if not "%FCP_MODEL_PULL_EXIT%"=="0" echo The final pull command exited with code %FCP_MODEL_PULL_EXIT%.
    echo Installed Ollama models:
    docker compose exec -T ollama ollama list
    exit /b 1
)

set /a FCP_MODEL_ATTEMPT+=1
powershell -NoProfile -Command "Start-Sleep -Seconds 3"
goto :pull_ollama_model

:reset_device_state
echo.
echo FRESH DEVICE INSTALL
echo This permanently removes this checkout's mutable FCP application state:
echo   - human administrators, passwords, authentication secrets, and login sessions
echo   - FCP device identity and keys
echo   - Federation membership, trust, pairing, discovery, onboarding, and authority state
echo   - capability, contribution, benchmark, provider, Activity, and job state
echo   - source configuration and recorder configuration, checkpoints, status, and runtime state
echo   - analyses, results, digital-twin projections, and retained legacy setup state
echo.
echo It preserves the machine recording corpus and its integrity metadata.
echo Docker images, downloaded model volumes, source code, and deployment settings are not application state and are not reset.
echo.
set "FCP_RESET_CONFIRM="
set /p "FCP_RESET_CONFIRM=Type RESET to continue: "
if /I not "%FCP_RESET_CONFIRM%"=="RESET" (
    echo Fresh install cancelled. No state was removed.
    exit /b 2
)

echo.
echo Stopping FCP before resetting mutable application state...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\stop_fcp_for_fresh_reset.ps1"
if errorlevel 1 (
    echo FCP containers could not be stopped safely. Nothing else was removed.
    pause
    exit /b 1
)

echo Resolving and clearing mutable FCP state while preserving recordings...
docker compose run --rm --no-deps --build --entrypoint python flask -m catalog.flask_app.services.device_state_reset
if errorlevel 1 (
    echo.
    echo Fresh factory reset did not complete. Review the specific path or recording-integrity error above.
    echo No FCP service will be started from an unverified reset.
    pause
    exit /b 1
)

echo Verifying factory-reset state before any FCP service can recreate runtime state...
docker compose run --rm --no-deps --entrypoint python flask -m catalog.flask_app.services.device_state_reset --verify-fresh
if errorlevel 1 (
    echo.
    echo Fresh factory reset could not be verified.
    echo No FCP service will be started. Review the specific verification failure above.
    pause
    exit /b 1
)

echo Fresh factory reset completed and verified. FCP will now start with first-administrator bootstrap.
echo.
exit /b 0

:show_help
echo Usage:
echo   start.cmd            Start FCP and preserve all existing state.
echo   start.cmd --resume   Reconnect, reuse saved capability evidence, then start FCP.
echo   start.cmd --fresh    Factory-reset mutable FCP state, verify it, then start FCP.
echo.
echo Normal and resume modes preserve identity, Federation membership, recordings,
echo source configuration, recorder checkpoints, results, and downloaded models.
echo Resume mode never runs inspection or benchmarks and never replaces Federation authority.
echo All modes install and verify the exact configured Ollama benchmark model.
echo The supported launcher also keeps a bounded local host update agent running.
echo The --fresh option requires typing RESET. Machine recordings, integrity metadata,
echo and immutable checkout scaffolding survive within the mounted application roots.
exit /b 0

:usage_error
echo Unknown option: %~1
echo Run start.cmd --help for supported options.
exit /b 2