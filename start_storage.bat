@echo off
chcp 65001 > nul
echo ========================================================
echo   KHOI DONG NHO GON: POSTGRES + MINIO (~300MB RAM)
echo ========================================================
docker compose up -d postgres minio minio-init
echo.
echo [OK] Postgres: localhost:5432 (User: ecom, DB: ecom_warehouse)
echo [OK] MinIO Console: http://localhost:9001 (User: minioadmin)
echo.
pause
