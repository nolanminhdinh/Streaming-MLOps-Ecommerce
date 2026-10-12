@echo off
chcp 65001 > nul
echo ========================================================
echo   KHOI DONG TOAN BO HE THONG STREAMING MLOPS E-COMMERCE
echo ========================================================
docker info > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Engine chua san sang. Hay mo Docker Desktop va kiem tra loi khoi dong.
    pause
    exit /b 1
)
docker compose config --quiet
if errorlevel 1 (
    echo [ERROR] Cau hinh Docker Compose khong hop le.
    pause
    exit /b 1
)
docker compose up -d
if errorlevel 1 (
    echo [ERROR] Khoi dong Docker Compose that bai. Kiem tra log service truoc khi chay pipeline.
    pause
    exit /b 1
)
echo.
echo [OK] NiFi Flow:        http://localhost:8080/nifi
echo [OK] MinIO Data Lake:  http://localhost:9001
echo [OK] Grafana Dashboard:http://localhost:3000
echo [OK] Prometheus:       http://localhost:9090
echo [OK] Postgres:         localhost:5432
echo [INFO] Producer, consumers, ETL va training chay rieng theo docs/05-how-to-run/how-to-run-week10.md.
echo.
pause
