@echo off
title Stopping Demo Services
color 0C
cd /d "%~dp0\.."
echo Stopping all services...
docker compose -f docker-compose.full.yml down
echo Done.
pause
