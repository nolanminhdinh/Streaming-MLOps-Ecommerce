@echo off
chcp 65001 > nul
echo ======================================================================
echo   DANG CHAY KIEM TRA & DANH GIA TINH HIEU QUA LUONG DU LIEU MLOPS
echo ======================================================================
.venv\Scripts\python.exe scripts\verify_pipeline_effectiveness.py
echo.
pause
