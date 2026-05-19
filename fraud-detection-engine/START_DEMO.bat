@echo off
title OpenG2P Fraud Detection Engine - Demo Launcher
color 0A

echo.
echo  =====================================================
echo   OpenG2P Fraud Detection Engine v2 - Demo Launcher
echo  =====================================================
echo.

cd /d "%~dp0\.."

echo [1/4] Checking services...
docker ps --filter "name=fraud-engine" --format "{{.Names}}" | findstr fraud-engine > nul
if %errorlevel% neq 0 (
    echo  Starting services from docker-compose.full.yml...
    docker compose -f docker-compose.full.yml up -d
    timeout /t 30 /nobreak > nul
) else (
    echo  Services already running.
)

echo.
echo [2/4] Installing real-time trigger (safe to repeat)...
docker exec fraud-engine python scripts/install_pg_trigger.py

echo.
echo [3/4] Loading demo fraud scenarios...
docker exec fraud-engine python scripts/demo_fraud_scenarios.py

echo.
echo [4/4] Opening dashboard and API docs in browser...
start "" http://localhost:8501
timeout /t 2 /nobreak > nul
start "" http://localhost:8002/docs

echo.
echo  =====================================================
echo   DONE! Two tabs are now open:
echo.
echo   Dashboard:   http://localhost:8501
echo   API Docs:    http://localhost:8002/docs
echo   OpenG2P:     http://localhost:8069/web
echo.
echo   To stop all services later, run STOP_DEMO.bat
echo  =====================================================
echo.
pause
