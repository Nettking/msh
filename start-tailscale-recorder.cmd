@echo off
setlocal EnableExtensions
title FCP - Tailscale recorder

cd /d "%~dp0"

where powershell.exe >nul 2>&1
if errorlevel 1 (
    echo Windows PowerShell was not found. The recorder was not started.
    exit /b 2
)

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\start_tailscale_recorder.ps1" %*
exit /b %ERRORLEVEL%
