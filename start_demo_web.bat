@echo off
setlocal
chcp 65001 > nul
cd /d "%~dp0"
echo ========================================================
echo   FLOWLAB - TRANG DEMO BAO VE STREAMING MLOPS
echo ========================================================
docker info >nul 2>&1
if errorlevel 1 (
  echo Docker chua san sang. Hay mo Docker Desktop, doi engine chay,
  echo sau do chay lai tep nay.
  pause
  exit /b 1
)
docker compose --profile demo up -d --build
if errorlevel 1 (
  echo Khoi dong chua thanh cong. Kiem tra thong bao ben tren.
  pause
  exit /b 1
)
echo.
echo Trang demo: http://127.0.0.1:8050
echo Doi worker va model khoi dong; trang se tu cap nhat trang thai.
start "" "http://127.0.0.1:8050"
pause
