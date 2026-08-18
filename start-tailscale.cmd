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
if not defined FCP_RELAY_BIND set "FCP_RELAY_BIND=%FCP_TAILSCALE_IP%"
if not defined FCP_DATA_DIR for %%I in ("%~dp0data") do set "FCP_DATA_DIR=%%~fI"

rem Discovery probes the normal peer web port without forcing the local launcher
rem to reserve that same port. start.cmd may therefore use its normal safe port
rem fallback when 5000 is occupied unless FCP_WEB_PORT was explicitly configured.
set "FCP_TAILSCALE_DISCOVERY_PORT=%FCP_WEB_PORT%"
if not defined FCP_TAILSCALE_DISCOVERY_PORT set "FCP_TAILSCALE_DISCOVERY_PORT=5000"

rem Keep the public-safe discovery snapshot outside data\federation. A --fresh
rem reset intentionally removes data\federation, but a fresh second device still
rem needs this snapshot on its login screen to find the existing Federation.
set "FCP_DISCOVERY_FILE=%FCP_DATA_DIR%\tailscale_discovery.json"
where python >nul 2>&1
if errorlevel 1 goto :skip_discovery

echo Discovering FCP Federations through the existing Tailscale login...
rem Run the standalone standard-library script directly. Using python -m here
rem imports catalog.federation.__init__ first and incorrectly makes optional
rem PostgreSQL/libpq support a host-side discovery prerequisite.
python "%~dp0catalog\federation\tailscale_host_discovery.py" --output "%FCP_DISCOVERY_FILE%" --web-port %FCP_TAILSCALE_DISCOVERY_PORT%
if errorlevel 1 echo Tailscale discovery failed safely; normal Federation onboarding remains available.

rem Automatic joining runs on the host because only here can tailscaled say who
rem a connecting peer really is. Docker Desktop rewrites the source address, so
rem the container sees the gateway and could never verify a peer. Both steps are
rem best effort: manual pairing codes keep working if either fails.
if not defined FCP_AUTO_JOIN_PORT set "FCP_AUTO_JOIN_PORT=5151"

rem The web and relay ports reach the tailnet because Docker Desktop creates its
rem own firewall rules. The responder is an ordinary host process and gets none,
rem so inbound joins would be dropped. Adding the rule needs elevation; without
rem it the responder still starts and the reason is printed rather than left as
rem an unexplained timeout on the joining device.
netsh advfirewall firewall show rule name="FCP tailnet join responder" >nul 2>&1
if errorlevel 1 (
    netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=%FCP_AUTO_JOIN_PORT% >nul 2>&1
    if errorlevel 1 (
        echo Could not open port %FCP_AUTO_JOIN_PORT% in Windows Firewall automatically.
        echo Run this once in an elevated prompt if the other device cannot join:
        echo   netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=%FCP_AUTO_JOIN_PORT%
    )
)

python "%~dp0catalog\federation\tailnet_join_responder.py" --check
if errorlevel 1 (
    echo Automatic Federation joining is unavailable; manual pairing codes still work.
) else (
    rem The responder replaces any earlier instance, writes its own pid file, and
    rem prints its own listening line only once the socket really exists.
    start "FCP tailnet join responder" /b python "%~dp0catalog\federation\tailnet_join_responder.py" --bind %FCP_TAILSCALE_IP% --port %FCP_AUTO_JOIN_PORT%
)

rem If another Federation was discovered and this device is not a member yet,
rem ask it to authorize this device. Nothing is written unless it agrees.
rem
rem Skipped for --fresh: the factory reset runs after this point and clears the
rem whole data directory, so a grant fetched now would be destroyed before it
rem could be redeemed. The next normal start joins automatically.
set "FCP_FRESH_REQUESTED="
for %%A in (%*) do if /i "%%~A"=="--fresh" set "FCP_FRESH_REQUESTED=1"
if defined FCP_FRESH_REQUESTED (
    echo Factory reset requested; automatic Federation joining runs on the next normal start.
) else (
    python "%~dp0catalog\federation\tailnet_join_client.py" --snapshot "%FCP_DISCOVERY_FILE%"
)

goto :start_fcp

:skip_discovery
echo Host Python was not found, so pre-start Tailscale discovery is skipped.
echo FCP will still bind to the Tailscale address and normal pairing remains available.

:start_fcp
echo FCP Tailscale address: %FCP_TAILSCALE_IP%
call "%~dp0start.cmd" %*
exit /b %ERRORLEVEL%
