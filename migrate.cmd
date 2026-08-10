@echo off
setlocal EnableExtensions
title FCP Existing Install Migration

cd /d "%~dp0"

where powershell >nul 2>&1
if errorlevel 1 (
    echo Windows PowerShell was not found.
    echo Run this migration from a supported Windows installation.
    pause
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\migrate_existing_fcp.ps1" -RepoRoot "%~dp0"
exit /b %ERRORLEVEL%
