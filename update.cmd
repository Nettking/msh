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
rem Keep selection in a subroutine rather than a parenthesized block: cmd.exe
rem expands %%VARIABLE%% references for a whole block before commands in that
rem block run, which can otherwise turn a newly assigned output path into "".
if defined MSH_RELAY_VOLUME_NAME goto :relay_state_explicit
call :select_relay_volume
if errorlevel 1 exit /b %ERRORLEVEL%
goto :relay_state_ready

:relay_state_explicit
echo.
echo Using explicitly selected Federation state: %MSH_RELAY_VOLUME_NAME%

:relay_state_ready
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

:select_relay_volume
set "MSH_RELAY_SELECTION_FILE=%TEMP%\msh-relay-selection-%RANDOM%-%RANDOM%.txt"
set "MSH_RELAY_SELECTION_EXIT="
if exist "%MSH_RELAY_SELECTION_FILE%" del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
echo.
echo Locating the saved Federation coordinator state...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\select_msh_relay_volume.ps1" -DataDirectory "%~dp0data" -OutputFile "%MSH_RELAY_SELECTION_FILE%"
set "MSH_RELAY_SELECTION_EXIT=%ERRORLEVEL%"
if "%MSH_RELAY_SELECTION_EXIT%"=="0" goto :relay_selection_read
if exist "%MSH_RELAY_SELECTION_FILE%" del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
echo Federation state selection stopped with exit code %MSH_RELAY_SELECTION_EXIT%.
pause
exit /b %MSH_RELAY_SELECTION_EXIT%

:relay_selection_read
if exist "%MSH_RELAY_SELECTION_FILE%" goto :relay_selection_load
echo Federation state selection did not create its result file.
pause
exit /b 1

:relay_selection_load
set /p "MSH_RELAY_VOLUME_NAME="<"%MSH_RELAY_SELECTION_FILE%"
del /q "%MSH_RELAY_SELECTION_FILE%" >nul 2>&1
if defined MSH_RELAY_VOLUME_NAME goto :relay_selection_success
echo Federation state selection returned an empty volume name.
pause
exit /b 1

:relay_selection_success
echo Federation state selected: %MSH_RELAY_VOLUME_NAME%
exit /b 0
