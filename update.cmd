@echo off
setlocal EnableExtensions
title MSH Update and Resume

cd /d "%~dp0"

where git >nul 2>&1
if errorlevel 1 (
    echo Git was not found.
    echo Install Git or open a terminal where Git is available.
    pause
    exit /b 1
)

echo Updating MSH with a safe fast-forward pull...
git pull --ff-only
if errorlevel 1 (
    echo.
    echo MSH was not changed because the repository could not be fast-forwarded safely.
    echo Commit, stash, or review local changes before trying again.
    pause
    exit /b 1
)

rem Windows PowerShell may otherwise emit redirected output in an encoding that
rem cmd.exe cannot parse with FOR /F. The runtime resolver also writes BOM-free
rem UTF-8 explicitly when an output file is provided.
chcp 65001 >nul

echo.
echo Starting MSH with the saved identity and Federation membership...
call start.cmd --resume
set "MSH_UPDATE_EXIT=%ERRORLEVEL%"
if not "%MSH_UPDATE_EXIT%"=="0" (
    echo.
    echo MSH update or resume stopped with exit code %MSH_UPDATE_EXIT%.
    pause
    exit /b %MSH_UPDATE_EXIT%
)

exit /b 0
