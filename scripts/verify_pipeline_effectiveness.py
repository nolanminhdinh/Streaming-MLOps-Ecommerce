"""
verify_pipeline_effectiveness.py
---------------------------------
Script Tự động Đo lường & Đánh giá Toàn diện Tính Hiệu quả của Luồng Dữ liệu MLOps:
1. Thông lượng sinh & chuẩn hóa dữ liệu (Throughput - records/sec)
2. Điểm số chất lượng & tỉ lệ bản ghi bị cách ly (Data Quality Score & Quarantine Rate)
3. Độ trễ xử lý từng mắt xích (Latency Breakdown - ms)
4. Độ chính xác phân loại ABC/XYZ & Quản trị tồn kho
5. Độ nhạy kiểm định phân phối Data Drift (KS-Test p-value)
6. Tổng kết bảng điểm đánh giá (Effectiveness Scorecard)
"""

import sys
import os
import time
from datetime import datetime, timezone
from dataclasses import asdict

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Add project root to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

import pandas as pd
import numpy as np
from data_simulator.data_simulator import ECommerceSimulator
from warehouse.etl.transform import unify_schema, generate_quality_report
from warehouse.etl.data_validation import DataValidator
from ml.features.abc_xyz import ABCXYZClassifier
from serving.app.model_loader import get_model_manager
from monitoring.evidently.drift_detector import DriftDetector

GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

def run_effectiveness_benchmark(sample_size: int = 1000):
    print(f"\n{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}{CYAN}   ĐÁNH GIÁ TÍNH HIỆU QUẢ LUỒNG DỮ LIỆU STREAMING MLOPS E-COMMERCE{RESET}")
    print(f"{BOLD}{CYAN}   Mẫu thử nghiệm: {sample_size:,} đơn hàng (Shopee & TikTok Shop){RESET}")
    print(f"{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════{RESET}\n")

    sim = ECommerceSimulator(shopee_ratio=0.55, cancel_rate=0.10)
    validator = DataValidator()
    model_mgr = get_model_manager()

    # -------------------------------------------------------------
    # 1. Đo lường Ingestion & Throughput
    # -------------------------------------------------------------
    print(f"{BOLD}[1/5] Kiểm tra Tốc độ Thu nạp & Chuẩn hóa Schema...{RESET}")
    now = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    raw_shopee = [asdict(sim._generate_shopee_event(now)) for _ in range(sample_size // 2)]
    raw_tiktok = [asdict(sim._generate_tiktok_event(now)) for _ in range(sample_size // 2)]
    all_raw = raw_shopee + raw_tiktok
    gen_time = time.perf_counter() - t0

    t1 = time.perf_counter()
    df_raw = pd.DataFrame(all_raw)
    df_unified = unify_schema(df_raw)
    unify_time = time.perf_counter() - t1

    ingestion_throughput = sample_size / (gen_time + unify_time)
    print(f"  ✓ Sinh và chuẩn hóa {sample_size:,} đơn hàng trong: {(gen_time + unify_time)*1000:.2f} ms")
    print(f"  ✓ Thông lượng đạt: {BOLD}{GREEN}{ingestion_throughput:,.0f} records/giây{RESET}")

    # -------------------------------------------------------------
    # 2. Kiểm tra Chất lượng Dữ liệu & Tỉ lệ Thất thoát
    # -------------------------------------------------------------
    print(f"\n{BOLD}[2/5] Kiểm định Tính Toàn vẹn & Điểm Chất lượng Dữ liệu...{RESET}")
    valid_df, invalid_df, report = validator.validate(df_unified)

    total_records = report.total_records
    valid_records = report.valid_records
    quarantine_rate = (len(invalid_df) / total_records) * 100.0 if total_records else 0.0
    quality_score = report.pass_rate * 100.0

    print(f"  ✓ Tổng số bản ghi tiếp nhận: {total_records:,}")
    print(f"  ✓ Số bản ghi hợp lệ:          {valid_records:,}")
    print(f"  ✓ Số bản ghi bị cách ly:      {len(invalid_df):,} ({quarantine_rate:.2f}%) — chưa nạp vào warehouse")
    print(f"  ✓ Điểm chất lượng schema:    {BOLD}{GREEN}{quality_score:.1f}/100 điểm{RESET}")

    # -------------------------------------------------------------
    # 3. Kiểm tra Tốc độ Phân loại ABC/XYZ & Tính Tồn kho
    # -------------------------------------------------------------
    print(f"\n{BOLD}[3/5] Đo lường Tốc độ Phân tích Ma trận ABC/XYZ & Tồn kho...{RESET}")
    skus = [f"SKU_{i:04d}" for i in range(1, 51)]
    dates = pd.date_range(end=datetime.now(), periods=30, freq="D")
    hist_records = []
    for s in skus:
        for d in dates:
            qty = max(0, int(np.random.normal(20, 5)))
            hist_records.append({
                "sku": s,
                "create_time": d,
                "quantity": qty,
                "buyer_total_amount": qty * 150000,
                "is_cancelled": False
            })
    df_hist = pd.DataFrame(hist_records)

    t2 = time.perf_counter()
    classifier = ABCXYZClassifier()
    matrix_df = classifier.fit_transform(df_hist)
    abc_time = (time.perf_counter() - t2) * 1000

    print(f"  ✓ Phân hạng 9 nhóm ma trận cho 50 SKUs x 30 ngày trong: {abc_time:.2f} ms")
    print(f"  ✓ Phân bổ hạng A: {len(matrix_df[matrix_df['abc_class'] == 'A'])} SKUs")

    # -------------------------------------------------------------
    # 4. Kiểm tra Độ trễ Suy luận Mô hình ML (Inference Latency)
    # -------------------------------------------------------------
    print(f"\n{BOLD}[4/5] Đo lường Độ trễ Suy luận Dự báo Nhu cầu (Inference)...{RESET}")
    from datetime import date
    today = date.today()
    latencies = []
    for _ in range(200):
        t_start = time.perf_counter()
        _ = model_mgr.predict_daily("ATN-CB-001", today)
        latencies.append((time.perf_counter() - t_start) * 1000)

    p50_latency = np.percentile(latencies, 50)
    p95_latency = np.percentile(latencies, 95)
    p99_latency = np.percentile(latencies, 99)

    print(f"  ✓ Độ trễ trung vị (P50): {BOLD}{GREEN}{p50_latency:.3f} ms{RESET}")
    print(f"  ✓ Độ trễ P95 in-process: {BOLD}{p95_latency:.3f} ms{RESET} (model_source={model_mgr.model_source})")
    print(f"  ✓ Độ trễ P99:           {BOLD}{GREEN}{p99_latency:.3f} ms{RESET}")

    # -------------------------------------------------------------
    # 5. Kiểm tra Độ nhạy Phát hiện Data Drift
    # -------------------------------------------------------------
    print(f"\n{BOLD}[5/5] Kiểm định Độ nhạy Giám sát Trôi dạt Dữ liệu (Data Drift)...{RESET}")
    drift_detector = DriftDetector()
    ref_data, curr_data = drift_detector.generate_synthetic_drift_data(n_samples=100, inject_drift=True)
    drift_report = drift_detector.detect_drift(ref_data, curr_data)
    drift_detected = drift_report.get("drift_detected", False)
    drift_share = drift_report.get("drift_share", 0.0)

    print(f"  ✓ Tỷ lệ đặc trưng phát hiện trôi dạt: {drift_share * 100:.1f}%")
    print(f"  ✓ Kết quả trên mẫu drift tổng hợp đã cấy: {BOLD}{'DETECTED' if drift_detected else 'NOT DETECTED'}{RESET}")

    # -------------------------------------------------------------
    # BẢNG ĐIỂM TỔNG KẾT HIỆU QUẢ (SCORECARD)
    # -------------------------------------------------------------
    print(f"\n{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}{CYAN}        BẢNG TỔNG KẾT ĐÁNH GIÁ TÍNH HIỆU QUẢ LUỒNG DỮ LIỆU (SCORECARD){RESET}")
    print(f"{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════{RESET}")
    print(f"┌──────────────────────────────────┬─────────────────┬──────────────┬────────────┐")
    print(f"│ CHỈ SỐ ĐO ĐƯỢC                   │ GIÁ TRỊ         │ MỤC TIÊU MẪU│ TRẠNG THÁI │")
    print(f"├──────────────────────────────────┼─────────────────┼──────────────┼────────────┤")
    print(f"│ 1. Chuẩn hóa in-process          │ {ingestion_throughput:>10,.0f} rec/s│ 2,000 rec/s  │ {'PASS' if ingestion_throughput >= 2000 else 'REVIEW':>10} │")
    print(f"│ 2. Bản ghi bị cách ly            │ {quarantine_rate:>10.2f} % │ 0.00 %       │ {'PASS' if quarantine_rate == 0 else 'REVIEW':>10} │")
    print(f"│ 3. Điểm chất lượng schema        │ {quality_score:>10.1f} /100│ 90.0 / 100   │ {'PASS' if quality_score >= 90 else 'REVIEW':>10} │")
    print(f"│ 4. P95 inference in-process      │ {p95_latency:>10.3f} ms│ 50.0 ms      │ {'PASS' if p95_latency < 50 else 'REVIEW':>10} │")
    print(f"│ 5. Drift trên dữ liệu tổng hợp   │ {drift_share*100:>10.1f} % │ detected     │ {'PASS' if drift_detected else 'REVIEW':>10} │")
    print(f"│ 6. ABC/XYZ (50 SKUs)             │ {abc_time:>10.2f} ms│ 200 ms       │ {'PASS' if abc_time < 200 else 'REVIEW':>10} │")
    print(f"└──────────────────────────────────┴─────────────────┴──────────────┴────────────┘")
    print(f"{BOLD}{YELLOW}  Các chỉ số trên là benchmark cục bộ; không đại diện cho độ trễ HTTP hay toàn tuyến Kafka → MinIO → PostgreSQL → MLflow → Serving. Các mục tiêu trong bảng là ngưỡng tham khảo của script.{RESET}\n")

if __name__ == "__main__":
    run_effectiveness_benchmark(1000)
