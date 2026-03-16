@echo off
echo Starting OpenG2P POC (prod compose)...
docker compose -f docker-compose.prod.yml --env-file .env up -d --build
echo Done. Odoo: http://localhost:8069
pause
