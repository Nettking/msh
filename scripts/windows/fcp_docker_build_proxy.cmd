@echo off
setlocal EnableExtensions

if not "%FCP_CONTROLLED_BUILD_ACTIVE%"=="1" goto :forward
if not defined FCP_REAL_DOCKER_EXE exit /b 64

if /I "%~1"=="compose" if /I "%~2"=="build" if /I "%~3"=="relay" if /I "%~4"=="flask" if /I "%~5"=="recorder" if "%~6"=="" goto :controlled_build
if /I "%~1"=="builder" if /I "%~2"=="prune" goto :cache_cleanup
if /I "%~1"=="compose" if /I "%~2"=="stop" if /I "%~3"=="flask" if "%~4"=="" goto :flask_stop
if /I "%~1"=="compose" if /I "%~2"=="up" if /I "%~3"=="-d" if /I "%~4"=="flask" if "%~5"=="" goto :flask_start

:forward
if not defined FCP_REAL_DOCKER_EXE exit /b 64
"%FCP_REAL_DOCKER_EXE%" %*
exit /b %ERRORLEVEL%

:flask_stop
"%FCP_REAL_DOCKER_EXE%" %*
set "FCP_FORWARD_EXIT=%ERRORLEVEL%"
if "%FCP_FORWARD_EXIT%"=="0" if defined FCP_ACTIVATION_PHASE_FILE >"%FCP_ACTIVATION_PHASE_FILE%" echo flask-stopped
exit /b %FCP_FORWARD_EXIT%

:flask_start
if defined FCP_ACTIVATION_PHASE_FILE >"%FCP_ACTIVATION_PHASE_FILE%" echo target-started
"%FCP_REAL_DOCKER_EXE%" %*
exit /b %ERRORLEVEL%

:controlled_build
if not defined FCP_CONTROLLED_BUILD_REPO_ROOT exit /b 64
if not defined FCP_CONTROLLED_BUILD_OUTPUT exit /b 64
if not defined FCP_BUILD_COMMIT exit /b 64
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0fcp_host_build.ps1" -RepoRoot "%FCP_CONTROLLED_BUILD_REPO_ROOT%" -OutputFile "%FCP_CONTROLLED_BUILD_OUTPUT%" -ExpectedCommit "%FCP_BUILD_COMMIT%" -LeaseAlreadyHeld
exit /b %ERRORLEVEL%

:cache_cleanup
if not defined FCP_CONTROLLED_BUILD_REPO_ROOT exit /b 64
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0fcp_host_build.ps1" -RepoRoot "%FCP_CONTROLLED_BUILD_REPO_ROOT%" -LeaseAlreadyHeld -CacheCleanupOnly
exit /b %ERRORLEVEL%
