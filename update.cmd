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

chcp 65001 >nul

rem Select the retained coordinator volume before start.cmd runs. Setting this
rem explicitly makes the general runtime resolver skip its legacy probe path.
if not defined MSH_RELAY_VOLUME_NAME (
    set "MSH_RELAY_SELECTION_FILE=%TEMP%\msh-relay-selection-%RANDOM%-%RANDOM%.txt"
    if exist "%MSH_RELAY_SELECTION_FILE%" del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
    echo.
    echo Locating the saved Federation coordinator state...
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\select_msh_relay_volume.ps1" -DataDirectory "%~dp0data" -OutputFile "%MSH_RELAY_SELECTION_FILE%"
    set "MSH_RELAY_SELECTION_EXIT=%ERRORLEVEL%"
    if not "%MSH_RELAY_SELECTION_EXIT%"=="0" (
        if exist "%MSH_RELAY_SELECTION_FILE%" del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
        echo Federation state selection stopped with exit code %MSH_RELAY_SELECTION_EXIT%.
        pause
        exit /b %MSH_RELAY_SELECTION_EXIT%
    )
    if not exist "%MSH_RELAY_SELECTION_FILE%" (
        echo Federation state selection did not create its result file.
        pause
        exit /b 1
    )
    set /p "MSH_RELAY_VOLUME_NAME="<"%MSH_RELAY_SELECTION_FILE%"
    del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
    if not defined MSH_RELAY_VOLUME_NAME (
        echo Federation state selection returned an empty volume name.
        pause
        exit /b 1
    )
    echo Federation state selected: %MSH_RELAY_VOLUME_NAME%
) else (
    echo.
    echo Using explicitly selected Federation state: %MSH_RELAY_VOLUME_NAME%
)

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
