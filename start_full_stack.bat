@echo off
chcp 65001 > nul
echo ========================================================
echo   KHOI DONG TOAN BO HE THONG STREAMING MLOPS E-COMMERCE
echo ========================================================
docker compose up -d
echo.
echo [OK] NiFi Flow:        http://localhost:8080/nifi
echo [OK] MinIO Data Lake:  http://localhost:9001
echo [OK] Grafana Dashboard:http://localhost:3000
echo [OK] Prometheus:       http://localhost:9090
echo [OK] Postgres:         localhost:5432
echo.
pause
