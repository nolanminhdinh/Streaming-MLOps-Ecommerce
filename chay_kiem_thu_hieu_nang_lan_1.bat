@echo off
chcp 65001 >nul
title KIỂM THỬ HIỆU NĂNG LUỒNG DỮ LIỆU STREAMING MLOPS LẦN 1
color 0B

echo ==============================================================================
echo        HỆ THỐNG MLOPS THỜI GIAN THỰC TMĐT - STRESS TEST RUN 1 (10,000 ORDERS)
echo ==============================================================================
echo.
echo [1/3] Kiểm tra môi trường ảo Python (.venv)...
if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Không tìm thấy Python tại .venv\Scripts\python.exe!
    pause
    exit /b 1
)

echo [2/3] Kiểm tra dịch vụ kho lưu trữ PostgreSQL & MinIO trong Docker...
docker compose up -d postgres minio minio-init >nul 2>&1
timeout /t 3 /nobreak >nul

echo [3/3] Bắt đầu thực thi kiểm thử hiệu năng luồng dữ liệu 8 chặng...
echo       Quy mô: 10,000 đơn hàng (Shopee + TikTok Shop) + 100 đơn lỗi DLQ
echo.
.venv\Scripts\python.exe scripts\run_heavy_pipeline_benchmark.py

echo.
echo ==============================================================================
echo  Báo cáo chi tiết đã được tạo tại: docs\test-pipeline\bao_cao_kiem_thu_hieu_nang_lan_1.md
echo ==============================================================================
echo.
pause
