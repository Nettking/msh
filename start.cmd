@echo off
title MSH Flask

cd /d "%~dp0"

docker info >nul 2>&1
if errorlevel 1 (
    echo Docker is not running.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

docker compose up --build flask

pause