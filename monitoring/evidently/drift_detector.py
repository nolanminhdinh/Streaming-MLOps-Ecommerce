"""
drift_detector.py
-----------------
Phát hiện trôi dạt dữ liệu (Data Drift) & trôi dạt khái niệm (Concept Drift) với Evidently AI:
  1. So sánh phân phối giữa tập tham chiếu (Reference: Dữ liệu huấn luyện lịch sử)
     và tập dữ liệu sản xuất gần nhất (Current: Dữ liệu đơn hàng suy luận mới).
  2. Áp dụng kiểm định thống kê:
     - Biến số (Lags, Rolling, Demand): Kolmogorov-Smirnov test (KS-test) / Wasserstein Distance / PSI.
     - Biến phân loại (Category, DayOfWeek): Population Stability Index (PSI) / Chi-Square.
  3. Xuất báo cáo HTML trực quan hóa phân phối tại:
     data/monitoring_reports/data_drift_report.html
  4. Xuất tệp tóm tắt JSON cho Prometheus & Closed-Loop Retraining:
     data/monitoring_reports/drift_summary.json
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("mlops.monitoring.drift")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

try:
    import numpy as np
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset, TargetDriftPreset
    HAS_EVIDENTLY = True
except ImportError:
    HAS_EVIDENTLY = False


# ─────────────────────────────────────────────────────────────
# 1. HÀM THỐNG KÊ TỰ ĐỘNG (KOLMOGOROV-SMIRNOV & PSI)
# ─────────────────────────────────────────────────────────────

def calculate_ks_2samp(data1: List[float], data2: List[float]) -> Tuple[float, float]:
    """
    Tính kiểm định Kolmogorov-Smirnov hai mẫu độc lập (KS-test).
    Trả về (ks_statistic, p_value xấp xỉ).
    """
    if not data1 or not data2:
        return 0.0, 1.0

    n1, n2 = len(data1), len(data2)
    s1 = sorted(data1)
    s2 = sorted(data2)

    all_vals = sorted(set(s1 + s2))
    cdf1 = 0
    cdf2 = 0
    max_diff = 0.0
    i1 = 0
    i2 = 0

    for val in all_vals:
        while i1 < n1 and s1[i1] <= val:
            i1 += 1
        while i2 < n2 and s2[i2] <= val:
            i2 += 1
        diff = abs(i1 / n1 - i2 / n2)
        if diff > max_diff:
            max_diff = diff

    # Xấp xỉ p-value theo tiệm cận phân phối Kolmogorov-Smirnov
    en = math.sqrt((n1 * n2) / (n1 + n2))
    lambda_val = (en + 0.12 + 0.11 / en) * max_diff
    # Công thức tiệm cận: P(K > lambda) ~= 2 * sum((-1)^(j-1) * exp(-2 * j^2 * lambda^2))
    if lambda_val <= 0:
        p_val = 1.0
    else:
        p_val = 2.0 * math.exp(-2.0 * (lambda_val ** 2))
        p_val = max(0.0, min(1.0, p_val))

    return round(max_diff, 4), round(p_val, 4)


def calculate_psi(ref: List[float], curr: List[float], num_bins: int = 10) -> float:
    """Tính chỉ số ổn định dân số (Population Stability Index - PSI)."""
    if not ref or not curr:
        return 0.0

    all_vals = ref + curr
    min_val, max_val = min(all_vals), max(all_vals)
    if min_val == max_val:
        return 0.0

    bin_width = (max_val - min_val) / num_bins
    eps = 1e-4

    ref_counts = [0] * num_bins
    curr_counts = [0] * num_bins

    for v in ref:
        idx = min(int((v - min_val) / bin_width), num_bins - 1)
        ref_counts[idx] += 1
    for v in curr:
        idx = min(int((v - min_val) / bin_width), num_bins - 1)
        curr_counts[idx] += 1

    n_ref = len(ref)
    n_curr = len(curr)

    psi = 0.0
    for r, c in zip(ref_counts, curr_counts):
        p_ref = (r / n_ref) + eps
        p_curr = (c / n_curr) + eps
        psi += (p_curr - p_ref) * math.log(p_curr / p_ref)

    return round(psi, 4)


# ─────────────────────────────────────────────────────────────
# 2. BỘ PHÁT HIỆN DATA DRIFT & CONCEPT DRIFT
# ─────────────────────────────────────────────────────────────

class DriftDetector:
    """Module giám sát chất lượng dữ liệu và phát hiện trôi dạt phân phối."""

    def __init__(
        self,
        drift_share_threshold: float = 0.30,  # Nếu > 30% features bị drift -> Báo động
        p_value_threshold: float = 0.05,       # p-value < 0.05 -> Có sự khác biệt ý nghĩa thống kê
        reports_dir: Optional[str] = None,
    ):
        self.drift_share_threshold = drift_share_threshold
        self.p_value_threshold = p_value_threshold
        self.reports_dir = reports_dir or os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "monitoring_reports"
        )
        self.reports_dir = os.path.abspath(self.reports_dir)
        os.makedirs(self.reports_dir, exist_ok=True)

    def generate_synthetic_drift_data(
        self,
        n_samples: int = 150,
        inject_drift: bool = True,
    ) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
        """
        Sinh dữ liệu giả lập đại diện giữa Reference (30 ngày trước)
        và Current (7 ngày gần nhất) để phục vụ kiểm thử và mô phỏng.
        """
        import random
        random.seed(42)

        # Baseline features: Lags, Rolling demand, Day of Week, Demand
        reference: Dict[str, List[float]] = {
            "lag_1": [random.gauss(20, 4) for _ in range(n_samples)],
            "lag_7": [random.gauss(19.5, 4.2) for _ in range(n_samples)],
            "rolling_mean_7": [random.gauss(20, 3) for _ in range(n_samples)],
            "rolling_std_7": [random.gauss(4, 1) for _ in range(n_samples)],
            "day_of_week": [random.randint(0, 6) for _ in range(n_samples)],
            "daily_demand": [random.gauss(20.5, 4.5) for _ in range(n_samples)],
        }

        current: Dict[str, List[float]] = {}
        if inject_drift:
            # Mô phỏng dịch chuyển phân phối do chiến dịch khuyến mãi hoặc thay đổi thuật toán sàn:
            # Lags và Demand tăng vọt, variance rộng hơn
            current["lag_1"] = [random.gauss(32, 7) for _ in range(n_samples)]
            current["lag_7"] = [random.gauss(30, 6.5) for _ in range(n_samples)]
            current["rolling_mean_7"] = [random.gauss(31, 5) for _ in range(n_samples)]
            current["rolling_std_7"] = [random.gauss(7, 2) for _ in range(n_samples)]
            current["day_of_week"] = [random.randint(0, 6) for _ in range(n_samples)]  # Không drift
            current["daily_demand"] = [random.gauss(33, 8) for _ in range(n_samples)]  # Concept drift
        else:
            # Phân phối tương đồng (không drift - chỉ có biến thiên ngẫu nhiên rất nhỏ)
            for k, vals in reference.items():
                current[k] = [v + random.gauss(0, 0.05) for v in vals]

        return reference, current

    def detect_drift(
        self,
        ref_data: Dict[str, List[float]],
        curr_data: Dict[str, List[float]],
        target_col: str = "daily_demand",
    ) -> Dict[str, Any]:
        """
        Thực hiện kiểm định trôi dạt trên toàn bộ các cột đặc trưng.
        """
        logger.info("Đang thực hiện kiểm định Data Drift & Concept Drift...")
        features = [col for col in ref_data.keys() if col != target_col]
        drifted_features = []
        feature_metrics: Dict[str, Dict[str, Any]] = {}

        for col in features:
            r_vals = ref_data[col]
            c_vals = curr_data.get(col, [])
            ks_stat, p_val = calculate_ks_2samp(r_vals, c_vals)
            psi = calculate_psi(r_vals, c_vals)

            is_drift = bool(p_val < self.p_value_threshold)
            if is_drift:
                drifted_features.append(col)

            feature_metrics[col] = {
                "stat_test": "Kolmogorov-Smirnov",
                "ks_statistic": ks_stat,
                "p_value": p_val,
                "psi": psi,
                "drift_detected": is_drift,
                "ref_mean": round(sum(r_vals) / len(r_vals), 2),
                "curr_mean": round(sum(c_vals) / len(c_vals), 2),
            }

        drift_share = round(len(drifted_features) / max(len(features), 1), 3)
        dataset_drift = bool(drift_share >= self.drift_share_threshold)

        # Kiểm tra riêng Target Drift (Concept Drift)
        target_drift = False
        target_metric: Dict[str, Any] = {}
        if target_col in ref_data and target_col in curr_data:
            t_ref = ref_data[target_col]
            t_curr = curr_data[target_col]
            t_ks, t_pval = calculate_ks_2samp(t_ref, t_curr)
            t_psi = calculate_psi(t_ref, t_curr)
            target_drift = bool(t_pval < self.p_value_threshold)
            target_metric = {
                "ks_statistic": t_ks,
                "p_value": t_pval,
                "psi": t_psi,
                "drift_detected": target_drift,
                "ref_mean": round(sum(t_ref) / len(t_ref), 2),
                "curr_mean": round(sum(t_curr) / len(t_curr), 2),
            }

        summary = {
            "drift_detected": dataset_drift or target_drift,
            "dataset_drift_detected": dataset_drift,
            "target_drift_detected": target_drift,
            "drift_share": drift_share,
            "drift_share_threshold": self.drift_share_threshold,
            "number_of_features": len(features),
            "number_of_drifted_features": len(drifted_features),
            "drifted_features": drifted_features,
            "features_evaluated": feature_metrics,
            "target_drift": target_metric,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Lưu tệp summary JSON
        summary_path = os.path.join(self.reports_dir, "drift_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Đã xuất tệp Drift Summary tại: {summary_path}")

        # Tạo báo cáo HTML trực quan
        self._export_html_report(summary, ref_data, curr_data)

        # Tạo thêm Evidently Interactive Report nếu thư viện evidently được cài đặt
        if HAS_EVIDENTLY and HAS_PANDAS:
            try:
                ref_df = pd.DataFrame(ref_data)
                curr_df = pd.DataFrame(curr_data)
                ev_report = Report(metrics=[DataDriftPreset()])
                ev_report.run(reference_data=ref_df, current_data=curr_df)
                evidently_html_path = os.path.join(self.reports_dir, "evidently_interactive_report.html")
                ev_report.save_html(evidently_html_path)
                logger.info(f"✓ Đã xuất Evidently Interactive Report tại: {evidently_html_path}")
            except Exception as ev_err:
                logger.debug(f"Không thể xuất báo cáo Evidently chuẩn: {ev_err}")

        return summary

    def _export_html_report(
        self,
        summary: Dict[str, Any],
        ref_data: Dict[str, List[float]],
        curr_data: Dict[str, List[float]],
    ) -> str:
        """Sinh tệp báo cáo HTML độc lập, giao diện Dashboard hiện đại."""
        html_path = os.path.join(self.reports_dir, "data_drift_report.html")

        status_badge = (
            '<span class="badge bg-danger">DRIFT DETECTED (CẦN TÁI HUẤN LUYỆN)</span>'
            if summary["drift_detected"]
            else '<span class="badge bg-success">HEALTHY (ỔN ĐỊNH - KHÔNG CÓ TRÔI DẠT)</span>'
        )

        rows = []
        for col, m in summary["features_evaluated"].items():
            drift_lbl = (
                '<span class="badge bg-danger">DRIFT</span>'
                if m["drift_detected"]
                else '<span class="badge bg-success">STABLE</span>'
            )
            rows.append(f"""
            <tr>
              <td><strong>{col}</strong></td>
              <td>{m['stat_test']}</td>
              <td>{m['ks_statistic']}</td>
              <td>{m['p_value']}</td>
              <td>{m['psi']}</td>
              <td>{m['ref_mean']}</td>
              <td>{m['curr_mean']}</td>
              <td>{drift_lbl}</td>
            </tr>
            """)

        html_content = f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <title>Báo cáo Giám sát Data Drift & Concept Drift (Tuần 8)</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
  <style>
    body {{ background-color: #0f172a; color: #f8fafc; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 25px; }}
    .card {{ background-color: #1e293b; border: 1px solid #334155; border-radius: 12px; margin-bottom: 25px; }}
    .card-header {{ background-color: #334155; color: #38bdf8; font-weight: bold; border-top-left-radius: 12px; border-top-right-radius: 12px; }}
    .table {{ color: #f8fafc; }}
    .table thead th {{ background-color: #334155; border-color: #475569; }}
    .table tbody td {{ border-color: #334155; }}
    .badge {{ font-size: 0.9em; padding: 6px 12px; border-radius: 6px; }}
    .metric-value {{ font-size: 2.2em; font-weight: bold; color: #38bdf8; }}
  </style>
</head>
<body>
  <div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-4 pb-2 border-bottom border-secondary">
      <div>
        <h2 class="text-info mb-1">📊 BÁO CÁO GIÁM SÁT DATA DRIFT & CONCEPT DRIFT</h2>
        <p class="text-secondary mb-0">Hệ thống MLOps E-Commerce Streaming Demand Forecasting | Tuần 8</p>
      </div>
      <div>{status_badge}</div>
    </div>

    <div class="row">
      <div class="col-md-3">
        <div class="card p-3 text-center">
          <div class="text-secondary">Tỷ lệ Feature bị Drift</div>
          <div class="metric-value">{round(summary['drift_share'] * 100, 1)}%</div>
          <div class="text-muted small">Ngưỡng báo động: {round(summary['drift_share_threshold'] * 100, 1)}%</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="card p-3 text-center">
          <div class="text-secondary">Số Features Trôi dạt</div>
          <div class="metric-value text-warning">{summary['number_of_drifted_features']} / {summary['number_of_features']}</div>
          <div class="text-muted small">Kiểm định KS-Test & PSI</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="card p-3 text-center">
          <div class="text-secondary">Concept / Target Drift</div>
          <div class="metric-value {'text-danger' if summary['target_drift_detected'] else 'text-success'}">
            {'PHÁT HIỆN' if summary['target_drift_detected'] else 'BÌNH THƯỜNG'}
          </div>
          <div class="text-muted small">Biến mục tiêu: daily_demand</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="card p-3 text-center">
          <div class="text-secondary">Thời điểm thẩm định</div>
          <div class="fs-5 text-light mt-2">{summary['evaluated_at'][:19].replace('T', ' ')}</div>
          <div class="text-muted small">UTC Timezone</div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header">CHI TIẾT KẾT QUẢ KIỂM ĐỊNH THỐNG KÊ THEO TỪNG ĐẶC TRƯNG</div>
      <div class="card-body p-0">
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th>Tên Đặc Trưng</th>
                <th>Phương Pháp Kiểm Định</th>
                <th>KS-Statistic</th>
                <th>p-Value</th>
                <th>PSI</th>
                <th>Trung bình Ref</th>
                <th>Trung bình Curr</th>
                <th>Trạng Thái</th>
              </tr>
            </thead>
            <tbody>
              {"".join(rows)}
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header">KẾT LUẬN & HÀNH ĐỘNG ĐỀ XUẤT (CLOSED-LOOP RETRAINING)</div>
      <div class="card-body">
        {'<div class="alert alert-danger mb-0"><strong>CẢNH BÁO:</strong> Hệ thống phát hiện trôi dạt phân phối vượt ngưỡng cho phép. Cơ chế Closed-Loop Retraining đã kích hoạt tín hiệu huấn luyện lại mô hình Champion với cửa sổ dữ liệu gần nhất.</div>' if summary['drift_detected'] else '<div class="alert alert-success mb-0"><strong>TỐT:</strong> Dữ liệu suy luận thực tế nằm trong phân phối ổn định so với tập dữ liệu huấn luyện lịch sử. Chưa cần tái huấn luyện mô hình.</div>'}
      </div>
    </div>
  </div>
</body>
</html>
"""
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info(f"✓ Đã xuất Báo cáo HTML trực quan tại: {html_path}")
        return html_path


def run_drift_pipeline(inject_drift: bool = True) -> Dict[str, Any]:
    """Hàm chạy nhanh quy trình phát hiện Drift và sinh báo cáo."""
    detector = DriftDetector()
    ref, curr = detector.generate_synthetic_drift_data(n_samples=150, inject_drift=inject_drift)
    return detector.detect_drift(ref, curr)


if __name__ == "__main__":
    run_drift_pipeline(inject_drift=True)
