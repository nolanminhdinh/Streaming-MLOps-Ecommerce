@echo off
chcp 65001 > nul
echo ========================================================
echo   KHOI DONG STREAMING TRUC QUAN: KAFKA + NIFI + MINIO
echo ========================================================
docker compose up -d zookeeper kafka kafka-init minio minio-init nifi
echo.
echo [OK] Apache NiFi Canvas: http://localhost:8080/nifi
echo [OK] MinIO Console:      http://localhost:9001
echo.
pause
