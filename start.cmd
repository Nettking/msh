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

rem Keep the human confirmation outside the host-mutation critical section so an
rem unattended --fresh prompt cannot block unrelated update activity indefinitely.
if "%FCP_FRESH_INSTALL%"=="1" if not "%FCP_FRESH_RESET_CONFIRMED%"=="1" (
    call :confirm_fresh_reset
    if errorlevel 1 exit /b 2
)

rem The outer invocation owns the one checkout mutation lease while an inner
rem start.cmd performs source repair/proof, build, reset/resume, every Compose
rem read, readiness, and update-agent startup. Existing update agents and the
rem native recorder updater therefore cannot change the checkout mid-activation.
if "%FCP_HOST_MUTATION_LEASE_ACTIVE%"=="1" goto :host_mutation_lease_ready
set "FCP_BUILD_COMMIT="
call :run_under_host_mutation_lease
set "FCP_LEASE_EXIT=%ERRORLEVEL%"
if not "%FCP_LEASE_EXIT%"=="0" pause
exit /b %FCP_LEASE_EXIT%

:host_mutation_lease_ready
rem A previous interrupted --fresh from an older build may have removed these
rem four Git-tracked runtime-root scaffolding files. They are immutable checkout
rem content, not FCP application state. Restore only missing canonical copies.
call :repair_checkout_scaffolding
if errorlevel 1 (
    echo.
    echo FCP could not restore immutable checkout scaffolding safely.
    call :maybe_pause
    exit /b 1
)

call :resolve_runtime_state
if errorlevel 1 (
    echo.
    echo FCP could not resolve its existing runtime state safely.
    call :maybe_pause
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
        call :maybe_pause
        exit /b 1
    )
)

if not defined FCP_BUILD_COMMIT (
    call :resolve_build_commit
    if errorlevel 1 (
        echo.
        echo FCP core images could not be built through the serialized host lifecycle.
        call :maybe_pause
        exit /b 1
    )
)

echo.
echo Starting the required FCP background services...
echo   - Federation relay
echo   - Managed recorder
docker compose up -d relay recorder
if errorlevel 1 (
    echo.
    echo Required FCP background services could not be started. Review the Docker error above.
    call :maybe_pause
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
    call :maybe_pause
    exit /b 1
)

rem The language model is an optional capability. Core FCP is already running
rem before Ollama or model installation, so Ollama image/service, model, or
rem network failure cannot block the workbench, Federation, recorder, or control surfaces.
set "FCP_AI_DEGRADED=0"
echo Starting optional Ollama service...
docker compose up -d ollama
if errorlevel 1 (
    set "FCP_AI_DEGRADED=1"
    echo.
    echo WARNING: Ollama is unavailable; core FCP remains running.
    echo AI capability can be repaired later without resetting Federation state.
) else (
    call :ensure_ollama_model
    if errorlevel 1 (
        set "FCP_AI_DEGRADED=1"
        echo.
        echo WARNING: AI capability is unavailable; core FCP remains running.
        echo Model installation can be retried later without resetting Federation state.
    )
)

echo.
docker compose ps relay ollama flask recorder
echo.

set "FCP_WEB_PORT_RESOLVED="
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$lines = @(docker compose port flask 5000); if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { exit 1 }; $binding = [string]$lines[0]; if ($binding -match ':(\d+)$') { $Matches[1] } else { exit 1 }"`) do set "FCP_WEB_PORT_RESOLVED=%%P"
if not defined FCP_WEB_PORT_RESOLVED (
    echo Could not determine the published Flask port.
    docker compose ps flask
    call :maybe_pause
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
    call :maybe_pause
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
rem The outer launcher still owns the host-mutation lease here. The new agent is
rem started only after every source-dependent Compose/readiness read is complete,
rem and its process does not inherit the internal lease marker.
call :start_update_agent
if errorlevel 1 (
    echo.
    echo The FCP host update agent could not be started safely.
    call :maybe_pause
    exit /b 1
)

echo.
echo FCP is running:        %FCP_BASE_URL%
echo Onboarding:            "%FCP_ONBOARDING_URL%"
echo Federation:            %FCP_BASE_URL%/federation
echo Recorder status:       %FCP_BASE_URL%/status
echo Documentation:         %FCP_BASE_URL%/docs
echo Device data:           %FCP_DATA_DIR%
echo Federation state:      %FCP_RELAY_VOLUME_NAME%
echo Running build commit:  %FCP_BUILD_COMMIT%
if "%FCP_AI_DEGRADED%"=="1" (
    echo AI capability:         unavailable; core FCP is healthy
) else (
    echo AI capability:         ready
)
echo.
echo Web and Federation relay access are limited to this FCP machine by default.
echo For Tailscale pairing, use start-tailscale.cmd.
echo For a trusted LAN, explicitly set both FCP_WEB_BIND and FCP_RELAY_BIND
echo to reachable private interfaces before start.cmd, then open FCP through that LAN address.
echo Setup, Federation identity, pairing state, recording state, checkpoints,
echo downloaded Ollama models, and recorded data are preserved between normal starts.
echo.

if /I "%FCP_SUPPRESS_BROWSER%"=="1" (
    echo Browser opening deferred to the calling startup orchestrator.
) else (
    start "" "%FCP_OPEN_URL%"
)
exit /b 0

:confirm_fresh_reset
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
set "FCP_FRESH_RESET_CONFIRMED=1"
exit /b 0

:run_under_host_mutation_lease
if not exist "%~dp0scripts\windows\fcp_host_activation_lease.ps1" (
    echo FCP host activation lease helper is missing.
    exit /b 1
)
set "FCP_LEASE_MODE=normal"
if "%FCP_FRESH_INSTALL%"=="1" set "FCP_LEASE_MODE=fresh"
if "%FCP_RESUME_EXISTING%"=="1" set "FCP_LEASE_MODE=resume"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\fcp_host_activation_lease.ps1" -RepoRoot "%~dp0" -Mode "%FCP_LEASE_MODE%"
exit /b %ERRORLEVEL%

:maybe_pause
if "%FCP_HOST_MUTATION_LEASE_ACTIVE%"=="1" exit /b 0
pause
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
if not exist "%~dp0scripts\windows\fcp_host_build.ps1" exit /b 1
set "FCP_BUILD_RESULT=%TEMP%\fcp-host-build-%RANDOM%-%RANDOM%.txt"
if exist "%FCP_BUILD_RESULT%" del /q "%FCP_BUILD_RESULT%" >nul 2>&1
set "FCP_BUILD_LEASE_ARG="
if "%FCP_HOST_MUTATION_LEASE_ACTIVE%"=="1" set "FCP_BUILD_LEASE_ARG=-LeaseAlreadyHeld"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\fcp_host_build.ps1" -RepoRoot "%~dp0" -OutputFile "%FCP_BUILD_RESULT%" %FCP_BUILD_LEASE_ARG%
set "FCP_BUILD_EXIT=%ERRORLEVEL%"
if not "%FCP_BUILD_EXIT%"=="0" (
    if exist "%FCP_BUILD_RESULT%" del /q "%FCP_BUILD_RESULT%" >nul 2>&1
    set "FCP_BUILD_COMMIT="
    exit /b %FCP_BUILD_EXIT%
)
if not exist "%FCP_BUILD_RESULT%" (
    echo Serialized host build did not create its commit result.
    set "FCP_BUILD_COMMIT="
    exit /b 1
)
set "FCP_BUILD_COMMIT="
set /p "FCP_BUILD_COMMIT="<"%FCP_BUILD_RESULT%"
del /q "%FCP_BUILD_RESULT%" >nul 2>&1
if not defined FCP_BUILD_COMMIT exit /b 1
powershell -NoProfile -Command "if ('%FCP_BUILD_COMMIT%' -match '^[0-9a-f]{40}$') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    set "FCP_BUILD_COMMIT="
    exit /b 1
)
echo Built FCP core images from %FCP_BUILD_COMMIT% through the serialized host lifecycle.
exit /b 0

:start_update_agent
if not exist "%~dp0scripts\windows\fcp_update_agent.ps1" exit /b 1
where powershell >nul 2>&1
if errorlevel 1 exit /b 1
set "FCP_LEASE_ACTIVE_SAVED=%FCP_HOST_MUTATION_LEASE_ACTIVE%"
set "FCP_LEASE_OWNER_SAVED=%FCP_HOST_MUTATION_LEASE_OWNER_PID%"
set "FCP_HOST_MUTATION_LEASE_ACTIVE="
set "FCP_HOST_MUTATION_LEASE_OWNER_PID="
start "FCP Update Agent" /b powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "%~dp0scripts\windows\fcp_update_agent.ps1" -RepoRoot "%~dp0" -DataDirectory "%FCP_DATA_DIR%" >nul 2>&1
set "FCP_AGENT_START_EXIT=%ERRORLEVEL%"
set "FCP_HOST_MUTATION_LEASE_ACTIVE=%FCP_LEASE_ACTIVE_SAVED%"
set "FCP_HOST_MUTATION_LEASE_OWNER_PID=%FCP_LEASE_OWNER_SAVED%"
set "FCP_LEASE_ACTIVE_SAVED="
set "FCP_LEASE_OWNER_SAVED="
if not "%FCP_AGENT_START_EXIT%"=="0" exit /b %FCP_AGENT_START_EXIT%
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

echo Ensuring optional Ollama model through host resource admission: %FCP_AI_MODEL_RESOLVED%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\fcp_model_pull.ps1" -RepoRoot "%~dp0" -Model "%FCP_AI_MODEL_RESOLVED%" -Target ollama
set "FCP_MODEL_PULL_EXIT=%ERRORLEVEL%"
if "%FCP_MODEL_PULL_EXIT%"=="0" (
    echo Ollama model is installed and verified.
    echo.
    exit /b 0
)
if "%FCP_MODEL_PULL_EXIT%"=="2" (
    echo AI model installation is paused by host resource pressure.
) else (
    echo AI model installation failed with code %FCP_MODEL_PULL_EXIT%.
)
exit /b 1

:reset_device_state
if not "%FCP_FRESH_RESET_CONFIRMED%"=="1" (
    call :confirm_fresh_reset
    if errorlevel 1 exit /b 2
)

rem Build while the current runtime is still available. The parent launcher owns
rem the host-mutation lease across this build, reset, and later activation. The
rem build helper therefore reuses that lease rather than reacquiring the mutex.
if not defined FCP_BUILD_COMMIT (
    call :resolve_build_commit
    if errorlevel 1 (
        echo FCP core images could not be built safely. No application state was removed.
        call :maybe_pause
        exit /b 1
    )
)

echo.
echo Stopping FCP before resetting mutable application state...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\stop_fcp_for_fresh_reset.ps1"
if errorlevel 1 (
    echo FCP containers could not be stopped safely. Nothing else was removed.
    call :maybe_pause
    exit /b 1
)

echo Resolving and clearing mutable FCP state while preserving recordings...
docker compose run --rm --no-deps --entrypoint python flask -m catalog.flask_app.services.device_state_reset
if errorlevel 1 (
    echo.
    echo Fresh factory reset did not complete. Review the specific path or recording-integrity error above.
    echo No FCP service will be started from an unverified reset.
    call :maybe_pause
    exit /b 1
)

echo Verifying factory-reset state before any FCP service can recreate runtime state...
docker compose run --rm --no-deps --entrypoint python flask -m catalog.flask_app.services.device_state_reset --verify-fresh
if errorlevel 1 (
    echo.
    echo Fresh factory reset could not be verified.
    echo No FCP service will be started. Review the specific verification failure above.
    call :maybe_pause
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
echo All modes attempt to install the configured Ollama model, but AI is optional for core startup.
echo The supported launcher also keeps a bounded local host update agent running.
echo The --fresh option requires typing RESET. Machine recordings, integrity metadata,
echo and immutable checkout scaffolding survive within the mounted application roots.
exit /b 0

:usage_error
echo Unknown option: %~1
echo Run start.cmd --help for supported options.
exit /b 2
