@echo off
setlocal EnableExtensions
cd /d "%~dp0"
where python >nul 2>&1
if errorlevel 1 (
  echo Python was not found.
  exit /b 1
)
python "%~dp0connect_recorder.py" %*
exit /b %ERRORLEVEL%
