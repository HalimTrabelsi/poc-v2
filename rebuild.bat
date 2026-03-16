@echo off
echo Rebuilding images (keeping volumes/data)...
docker compose -f docker-compose.prod.yml --env-file .env build --no-cache
docker compose -f docker-compose.prod.yml --env-file .env up -d
echo Rebuilt and started (data preserved).
pause
