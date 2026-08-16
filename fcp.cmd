@echo off
setlocal
python "%~dp0fcp.py" %*
exit /b %ERRORLEVEL%
