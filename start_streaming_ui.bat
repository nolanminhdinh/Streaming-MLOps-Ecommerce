@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ========================================================
echo   KHOI DONG LUONG DU LIEU CHINH
echo ========================================================
docker compose up -d --build zookeeper kafka kafka-init minio minio-init postgres pipeline-worker
if errorlevel 1 (
    echo [ERROR] Khoi dong that bai. Kiem tra log Docker.
    pause
    exit /b 1
)
echo.
echo [INFO] Worker tu chay hai consumer, ETL va xuat CSV.
echo [OK] MinIO Console:      http://localhost:9001
echo.
pause
