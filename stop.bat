@echo off
echo Stopping containers (keeping volumes/data)...
docker compose -f docker-compose.prod.yml --env-file .env down
echo Containers stopped.
pause
