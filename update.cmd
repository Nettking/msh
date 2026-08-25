@echo off
setlocal EnableExtensions
title FCP Update

echo.
echo update.cmd is retired and does not modify this checkout or runtime.
echo.
echo Use the FCP workbench instead:
echo   Federation ^> Check for updates ^> Update all devices
echo.
echo That is the supported update path for both single-device and multi-device Federations.
echo It validates the approved repository and main branch, a clean checkout,
echo the exact target commit, host resource admission, activation, and the
echo running runtime before reporting success.
echo.
echo For an ordinary restart without updating, run:
echo   start.cmd
echo or, when you explicitly want to verify saved setup before opening FCP:
echo   start.cmd --resume
echo.
exit /b 2
