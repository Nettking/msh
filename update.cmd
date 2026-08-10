@echo off
setlocal EnableExtensions
title FCP Update and Resume

cd /d "%~dp0"

where git >nul 2>&1
if errorlevel 1 (
    echo Git was not found.
    echo Install Git or open a terminal where Git is available.
    pause
    exit /b 1
)

echo Updating FCP with a safe fast-forward pull...
git pull --ff-only
if errorlevel 1 (
    echo.
    echo FCP was not changed because the repository could not be fast-forwarded safely.
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
if defined FCP_RELAY_VOLUME_NAME goto :relay_state_explicit
call :select_relay_volume
if errorlevel 1 exit /b %ERRORLEVEL%
goto :relay_state_ready

:relay_state_explicit
echo.
echo Using explicitly selected Federation state: %FCP_RELAY_VOLUME_NAME%

:relay_state_ready
echo.
echo Starting FCP with the saved identity and Federation membership...
call start.cmd --resume
set "FCP_UPDATE_EXIT=%ERRORLEVEL%"
if not "%FCP_UPDATE_EXIT%"=="0" (
    echo.
    echo FCP update or resume stopped with exit code %FCP_UPDATE_EXIT%.
    pause
    exit /b %FCP_UPDATE_EXIT%
)

exit /b 0

:select_relay_volume
set "FCP_RELAY_SELECTION_FILE=%TEMP%\fcp-relay-selection-%RANDOM%-%RANDOM%.txt"
set "FCP_RELAY_SELECTION_EXIT="
if exist "%FCP_RELAY_SELECTION_FILE%" del /q "%FCP_RELAY_SELECTION_FILE%" >nul 2>&1
echo.
echo Locating the saved Federation coordinator state...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\select_fcp_relay_volume.ps1" -DataDirectory "%~dp0data" -OutputFile "%FCP_RELAY_SELECTION_FILE%"
set "FCP_RELAY_SELECTION_EXIT=%ERRORLEVEL%"
if "%FCP_RELAY_SELECTION_EXIT%"=="0" goto :relay_selection_read
if exist "%FCP_RELAY_SELECTION_FILE%" del /q "%FCP_RELAY_SELECTION_FILE%" >nul 2>&1
echo Federation state selection stopped with exit code %FCP_RELAY_SELECTION_EXIT%.
pause
exit /b %FCP_RELAY_SELECTION_EXIT%

:relay_selection_read
if exist "%FCP_RELAY_SELECTION_FILE%" goto :relay_selection_load
echo Federation state selection did not create its result file.
pause
exit /b 1

:relay_selection_load
set /p "FCP_RELAY_VOLUME_NAME="<"%FCP_RELAY_SELECTION_FILE%"
del /q "%FCP_RELAY_SELECTION_FILE%" >nul 2>&1
if defined FCP_RELAY_VOLUME_NAME goto :relay_selection_success
echo Federation state selection returned an empty volume name.
pause
exit /b 1

:relay_selection_success
echo Federation state selected: %FCP_RELAY_VOLUME_NAME%
exit /b 0
