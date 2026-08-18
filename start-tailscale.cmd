@echo off
setlocal EnableExtensions
title FCP - Tailscale zero-touch startup

cd /d "%~dp0"

set "FCP_START_MODE="
set "FCP_INITIALIZE_FEDERATION="

:parse_args
if "%~1"=="" goto :arguments_ready
if /I "%~1"=="--fresh" (
    if defined FCP_START_MODE goto :usage_error
    set "FCP_START_MODE=--fresh"
    shift
    goto :parse_args
)
if /I "%~1"=="--resume" (
    if defined FCP_START_MODE goto :usage_error
    set "FCP_START_MODE=--resume"
    shift
    goto :parse_args
)
if /I "%~1"=="--initialize-federation" (
    set "FCP_INITIALIZE_FEDERATION=1"
    shift
    goto :parse_args
)
if /I "%~1"=="--help" goto :show_help
if /I "%~1"=="/?" goto :show_help
goto :usage_error

:arguments_ready
where tailscale >nul 2>&1
if errorlevel 1 (
    echo Tailscale was not found. Zero-touch Federation startup requires Tailscale.
    echo Use start.cmd only for a deliberately standalone installation.
    exit /b 2
)

set "FCP_TAILSCALE_IP="
for /f "usebackq delims=" %%I in (`tailscale ip -4 2^>nul`) do if not defined FCP_TAILSCALE_IP set "FCP_TAILSCALE_IP=%%I"
if not defined FCP_TAILSCALE_IP (
    echo Tailscale is installed but no logged-in IPv4 tailnet address is available.
    exit /b 2
)

if not defined FCP_WEB_BIND set "FCP_WEB_BIND=%FCP_TAILSCALE_IP%"
if not defined FCP_RELAY_BIND set "FCP_RELAY_BIND=%FCP_TAILSCALE_IP%"
if not defined FCP_WEB_PORT set "FCP_WEB_PORT=5000"
if not defined FCP_DATA_DIR for %%I in ("%~dp0data") do set "FCP_DATA_DIR=%%~fI"
if not defined FCP_AUTO_JOIN_PORT set "FCP_AUTO_JOIN_PORT=5151"
if not defined FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED set "FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED=1"
if not defined FCP_FEDERATION_STORAGE_AUTHORITY_RELAY set "FCP_FEDERATION_STORAGE_AUTHORITY_RELAY=ws://relay:8765"
set "FCP_TAILSCALE_DISCOVERY_PORT=%FCP_WEB_PORT%"
set "FCP_AUTO_JOIN_APP_URL=http://%FCP_TAILSCALE_IP%:%FCP_WEB_PORT%"

where python >nul 2>&1
if errorlevel 1 (
    echo Host Python 3 is required for zero-touch Tailscale startup.
    exit /b 2
)

rem The reset belongs to start.cmd. Discovery and enrollment deliberately happen
rem afterwards, because --fresh removes all mutable data except the recording
rem corpus and therefore must not be handed a pre-reset pairing grant.
if defined FCP_START_MODE (
    call "%~dp0start.cmd" %FCP_START_MODE%
) else (
    call "%~dp0start.cmd"
)
if errorlevel 1 exit /b %ERRORLEVEL%

rem The responder is a host process, so Docker Desktop does not create its
rem firewall rule. Keep this actionable and idempotent.
netsh advfirewall firewall show rule name="FCP tailnet join responder" >nul 2>&1
if errorlevel 1 (
    netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=%FCP_AUTO_JOIN_PORT% >nul 2>&1
    if errorlevel 1 (
        echo Could not open port %FCP_AUTO_JOIN_PORT% in Windows Firewall automatically.
        echo Run this once in an elevated prompt if another trusted device cannot join:
        echo   netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=%FCP_AUTO_JOIN_PORT%
    )
)

python "%~dp0scripts\federation_host_runner.py" tailnet_join_responder --check
if errorlevel 1 (
    echo Automatic Federation joining is unavailable; refusing zero-touch startup.
    exit /b 2
)
start "FCP tailnet join responder" /b python "%~dp0scripts\federation_host_runner.py" tailnet_join_responder --bind %FCP_TAILSCALE_IP% --port %FCP_AUTO_JOIN_PORT% --app-url %FCP_AUTO_JOIN_APP_URL%

set "FCP_ZERO_TOUCH_ARGS="
if defined FCP_INITIALIZE_FEDERATION set "FCP_ZERO_TOUCH_ARGS=--initialize-federation"
python "%~dp0scripts\zero_touch_federation_start.py" %FCP_ZERO_TOUCH_ARGS% --web-port %FCP_TAILSCALE_DISCOVERY_PORT%
if errorlevel 1 (
    echo.
    echo FCP services are running, but zero-touch Federation setup did not complete.
    echo Review the error above; no fallback Federation was created.
    exit /b 2
)

echo.
echo FCP Tailscale address: %FCP_TAILSCALE_IP%
echo Zero-touch startup is complete.
start "" "http://%FCP_TAILSCALE_IP%:%FCP_TAILSCALE_DISCOVERY_PORT%/federation"
exit /b 0

:show_help
echo Usage:
echo   start-tailscale.cmd --fresh --initialize-federation
echo      First device only: reset, create the Federation, and prompt once for the first admin.
echo   start-tailscale.cmd --fresh
echo      New trusted device: reset, discover, join, benchmark, and activate services automatically.
echo   start-tailscale.cmd
echo      Normal restart: reuse membership and current capability evidence.
echo   start-tailscale.cmd --resume
echo      Explicit saved-state reconnect before normal zero-touch reconciliation.
exit /b 0

:usage_error
echo Unknown or conflicting option: %~1
echo Run start-tailscale.cmd --help for supported options.
exit /b 2
