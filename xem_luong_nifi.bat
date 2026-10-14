@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Mo NiFi: http://localhost:8080/nifi/
echo Demo gui 64 su kien, cach nhau 0.8 giay. Bam Refresh tren canvas de xem In/Out.
echo Sau do script doi soat MinIO, ETL, warehouse va API. Du lieu demo duoc giu lai.
start "" "http://localhost:8080/nifi/"
if not exist ".venv\Scripts\python.exe" (
    echo Khong tim thay Python .venv cua du an.
    pause
    exit /b 1
)
".venv\Scripts\python.exe" scripts\run_nifi_live_demo.py
if errorlevel 1 (
    echo Demo chua dat. Xem loi phia tren; cac dich vu Docker can dang chay.
) else (
    echo Demo PASS. Mo MinIO http://localhost:9001/ de xem raw/nifi/orders/.
)
pause
