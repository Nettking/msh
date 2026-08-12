@echo off
setlocal EnableExtensions
title FCP - Tailscale discovery

cd /d "%~dp0"

where tailscale >nul 2>&1
if errorlevel 1 (
    echo Tailscale was not found. Falling back to the normal FCP launcher.
    call "%~dp0start.cmd" %*
    exit /b %ERRORLEVEL%
)

set "FCP_TAILSCALE_IP="
for /f "usebackq delims=" %%I in (`tailscale ip -4 2^>nul`) do if not defined FCP_TAILSCALE_IP set "FCP_TAILSCALE_IP=%%I"
if not defined FCP_TAILSCALE_IP (
    echo Tailscale is installed but no logged-in IPv4 tailnet address is available.
    echo Falling back to the normal FCP launcher.
    call "%~dp0start.cmd" %*
    exit /b %ERRORLEVEL%
)

if not defined FCP_WEB_BIND set "FCP_WEB_BIND=%FCP_TAILSCALE_IP%"
if not defined FCP_WEB_PORT set "FCP_WEB_PORT=5000"
if not defined FCP_DATA_DIR for %%I in ("%~dp0data") do set "FCP_DATA_DIR=%%~fI"

set "FCP_DISCOVERY_FILE=%FCP_DATA_DIR%\federation\onboarding\tailscale_discovery.json"
where python >nul 2>&1
if errorlevel 1 goto :skip_discovery

echo Discovering FCP Federations through the existing Tailscale login...
python -m catalog.federation.tailscale_host_discovery --output "%FCP_DISCOVERY_FILE%" --web-port %FCP_WEB_PORT%
if errorlevel 1 echo Tailscale discovery failed safely; normal Federation onboarding remains available.
goto :start_fcp

:skip_discovery
echo Host Python was not found, so pre-start Tailscale discovery is skipped.
echo FCP will still bind to the Tailscale address and normal pairing remains available.

:start_fcp
echo FCP Tailscale address: %FCP_TAILSCALE_IP%
call "%~dp0start.cmd" %*
exit /b %ERRORLEVEL%
