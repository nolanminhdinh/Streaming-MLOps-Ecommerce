"""
demo_pipeline_flow.py
---------------------
Kịch bản minh họa logic nội bộ MLOps (không kết nối Kafka/MinIO/PostgreSQL/MLflow):
  Tầng 1: Data Simulator (Shopee 84 cột & TikTok 71 cột)
  Tầng 2: Data Unification & Validation (Adapter + Data Quality Scorecard)
  Tầng 3: Feature Engineering & Ma trận 9 ô ABC/XYZ
  Tầng 4: Model Serving & Quản trị Tồn kho Động (SS & ROP Alert)
  Tầng 5: Minh họa phát hiện drift trên dữ liệu tổng hợp (không chạy retraining)
  Tầng 6: Minh họa chuẩn hóa payload mẫu trong bộ nhớ (không gọi webhook thật)
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# Thiết lập encoding UTF-8 cho console
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Thêm thư mục gốc vào PYTHONPATH
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from data_simulator.data_simulator import ECommerceSimulator, Platform
from warehouse.etl.transform import unify_schema, clean_data
from warehouse.etl.data_validation import DataValidator
from ml.features.abc_xyz import ABCXYZClassifier
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from serving.app.inventory_service import InventoryService
from serving.app.model_loader import get_model_manager
from monitoring.evidently.drift_detector import DriftDetector

logging.basicConfig(level=logging.WARNING)

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_banner(title: str, step_num: int):
    print(f"\n{CYAN}{'═' * 75}{RESET}")
    print(f"{BOLD}{CYAN}  BƯỚC {step_num}: {title.upper()}{RESET}")
    print(f"{CYAN}{'═' * 75}{RESET}")


def step_1_streaming_simulation():
    print_banner("Mô phỏng luồng đơn hàng thời gian thực (Streaming Ingestion)", 1)
    print("▶ Đang khởi tạo bộ sinh dữ liệu TMĐT đa kênh Việt Nam (Shopee + TikTok Shop)...")

    sim = ECommerceSimulator(shopee_ratio=0.5, cancel_rate=0.1)

    from dataclasses import asdict
    now = datetime.now(timezone.utc)
    shopee_event = asdict(sim._generate_shopee_event(now))
    tiktok_event = asdict(sim._generate_tiktok_event(now))

    print(f"\n{GREEN}✔ Đã phát sinh 2 sự kiện đơn hàng chuẩn XomData Schema:{RESET}")
    print(f"  • {BOLD}Payload Shopee ({len(shopee_event)} trường trong simulator):{RESET}")
    print(f"    - Mã đơn:          {shopee_event.get('order_sn')}")
    print(f"    - Sản phẩm (SKU):  {shopee_event.get('item_name')} ({shopee_event.get('item_sku')})")
    print(f"    - Trạng thái:      {shopee_event.get('order_status')}")
    print(f"    - Giá bán:         {shopee_event.get('original_price'):,} VND")
    print(f"    - Khuyến mãi:      Người bán trợ giá {shopee_event.get('voucher_from_seller', 0):,} VND | Sàn trợ giá {shopee_event.get('voucher_from_shopee', 0):,} VND")
    print(f"    - Địa chỉ giao:    {shopee_event.get('district')}, {shopee_event.get('state')} (ĐVVC: {shopee_event.get('shipping_carrier')})")

    print(f"\n  • {BOLD}Payload TikTok Shop ({len(tiktok_event)} trường trong simulator):{RESET}")
    print(f"    - Mã đơn:          {tiktok_event.get('order_id')}")
    print(f"    - Sản phẩm (SKU):  {tiktok_event.get('product_name')} ({tiktok_event.get('seller_sku')})")
    print(f"    - Trạng thái:      {tiktok_event.get('order_status')}")
    print(f"    - Tổng thanh toán: {tiktok_event.get('total_amount', 0):,} VND")
    print(f"    - Địa chỉ giao:    {tiktok_event.get('district')}, {tiktok_event.get('region_state')} (ĐVVC: {tiktok_event.get('shipping_provider')})")

    print(f"\n{YELLOW}➔ Phạm vi bước này:{RESET} chỉ tạo 2 payload trong bộ nhớ. Script demo này không gửi chúng tới Kafka hoặc MinIO.")
    return [shopee_event, tiktok_event]


def step_2_unify_and_validate(raw_events):
    print_banner("Chuẩn hóa lược đồ (Adapter) & Kiểm định chất lượng dữ liệu", 2)
    print("▶ Đưa dữ liệu thô qua hàm unify_schema() để ánh xạ về cấu trúc Star Schema...")

    raw_df = pd.DataFrame(raw_events)
    unified_df = unify_schema(raw_df)

    print(f"{GREEN}✔ Đã chuyển payload mẫu Shopee/TikTok sang lược đồ trung gian của ETL:{RESET}")
    display_cols = ["order_id", "platform", "sku", "quantity", "buyer_total_amount", "order_status", "carrier_name"]
    print(unified_df[display_cols].to_string(index=False))

    print("\n▶ Chạy module DataValidator kiểm định tính toàn vẹn...")
    validator = DataValidator()
    valid_df, invalid_df, report = validator.validate(unified_df)

    print(f"{GREEN}✔ Kết quả Kiểm định Chất lượng Dữ liệu (Data Quality Scorecard):{RESET}")
    print(f"  • Tổng số bản ghi:      {report.total_records}")
    print(f"  • Bản ghi hợp lệ:       {report.valid_records} ({report.pass_rate * 100:.1f}%)")
    print(f"  • Bản ghi bị cách ly:   {report.invalid_records}")
    print(f"  • Cơ chế bảo vệ DWH:    Ràng buộc UNIQUE (order_id, platform) + ON CONFLICT DO NOTHING.")


def step_3_feature_store_and_abc_xyz():
    print_banner("Feature Store & Phân hạng ma trận 9 ô ABC/XYZ", 3)
    print("▶ Dựng chuỗi thời gian TỔNG HỢP 30 ngày trong bộ nhớ cho vài SKU minh họa...")

    # Tạo dữ liệu giả lập 30 ngày cho 3 nhóm SKU đặc trưng
    dates = pd.date_range(end=datetime.now(), periods=30, freq="D")
    records = []

    # SKU 1: Áo thun basic (Doanh thu cao, ổn định -> Nhóm AX)
    for d in dates:
        records.append({
            "sku": "ATN-CB-001",
            "create_time": d,
            "quantity": int(np.random.normal(30, 2)),
            "buyer_total_amount": 30 * 129000,
            "is_cancelled": False,
        })
    # SKU 2: Đầm dự tiệc (Doanh thu khá, biến động cao -> Nhóm BY)
    for d in dates:
        qty = int(np.random.choice([2, 18, 5, 25]))
        records.append({
            "sku": "DRS-MD-001",
            "create_time": d,
            "quantity": qty,
            "buyer_total_amount": qty * 350000,
            "is_cancelled": False,
        })
    # SKU 3: Phụ kiện ốp lưng (Doanh thu thấp, không đều -> Nhóm CZ)
    for d in dates:
        qty = 1 if d.day % 4 == 0 else 0
        records.append({
            "sku": "OL-IP15PM",
            "create_time": d,
            "quantity": qty,
            "buyer_total_amount": qty * 59000,
            "is_cancelled": False,
        })

    hist_df = pd.DataFrame(records)
    classifier = ABCXYZClassifier()
    matrix_df = classifier.fit_transform(hist_df)

    print(f"{GREEN}✔ Kết quả phân loại ma trận 9 ô Pareto ABC/XYZ:{RESET}")
    cols_show = ["sku", "total_revenue", "rev_share", "cv", "abc_class", "xyz_class", "matrix_class", "safety_stock", "reorder_point"]
    print(matrix_df[cols_show].to_string(index=False))

    print(f"\n{YELLOW}➔ Ý nghĩa nghiệp vụ:{RESET}")
    print(f"  • Nhóm AX (ATN-CB-001): Doanh thu chủ lực, ổn định -> Cấp độ phục vụ 98-99%, hạn chế tối đa đứt hàng.")
    print(f"  • Nhóm CZ (OL-IP15PM):  Doanh thu thấp, thất thường -> Duy trì mức an toàn tối thiểu để tránh tồn đọng vốn.")


def step_4_model_serving_and_inventory():
    print_banner("FastAPI Model Serving & Quản trị Tồn kho Động", 4)
    print("▶ Đọc trạng thái hiện tại của Model Manager...")

    model_mgr = get_model_manager()
    meta = model_mgr.get_metadata()
    algo = meta.get('champion_algorithm', 'LightGBM_Tuned')
    wape = meta.get('metrics', {}).get('cv_wape')
    version = meta.get('version', '0')
    model_state = "ML model" if model_mgr.has_model else "heuristic fallback"
    wape_text = f" | CV WAPE: {wape}%" if wape is not None else " | CV WAPE: unavailable"
    print(f"{GREEN}• Model state: {model_state} | source: {model_mgr.model_source} | {algo} | Version: {version}{wape_text}{RESET}")

    # Dự báo sản lượng cho SKU 'DRS-MD-001' trong 7 ngày tới
    from datetime import date
    start_d = datetime.now().date()
    end_d = start_d + timedelta(days=6)
    preds = model_mgr.predict_range(sku="DRS-MD-001", from_date=start_d, to_date=end_d, confidence_interval=True)
    print(f"\n{GREEN}✔ Dự báo sản lượng 7 ngày tới cho SKU DRS-MD-001 (Đầm Midi Nữ):{RESET}")
    for p in preds:
        print(f"  • {p['date']}: Dự báo {p['predicted_quantity']} sản phẩm | KTC 95%: [{p['lower_bound']} - {p['upper_bound']}]")

    print("\n▶ Kích hoạt Dịch vụ Quản trị Tồn kho Động (Inventory Optimization Service)...")
    inv_service = InventoryService()
    # Giả định: Tồn kho hiện tại = 12 cái, Lead Time = 3 ngày, Service Level = 95%
    alert_item = inv_service.evaluate_sku(
        sku="DRS-MD-001",
        current_stock=12,
        lead_time_days=3,
        service_level=0.95,
    )

    print(f"{GREEN}✔ Kết quả tính toán Tồn kho Ngẫu nhiên (Stochastic Inventory Policy):{RESET}")
    print(f"  • Tồn kho hiện tại:       {BOLD}{alert_item.current_stock} sản phẩm{RESET}")
    print(f"  • Tồn kho an toàn (SS):   {BOLD}{alert_item.safety_stock} sản phẩm{RESET} (Công thức: Z * sigma * sqrt(L))")
    print(f"  • Điểm đặt hàng lại (ROP): {BOLD}{alert_item.reorder_point} sản phẩm{RESET} (Công thức: mu * L + SS)")
    print(f"  • Cấp độ cảnh báo:        {RED}{BOLD}{alert_item.alert_level}{RESET} (Current Stock <= SS -> Nguy cơ đứt hàng khẩn cấp!)")
    print(f"  • Dự báo cạn kho sau:     {BOLD}{alert_item.days_until_stockout} ngày{RESET}")
    print(f"  • Đề xuất nhập bổ sung:   {BOLD}{alert_item.recommended_reorder_qty} sản phẩm{RESET}")


def step_5_monitoring_and_retraining():
    print_banner("Giám sát Data Drift (Evidently AI) & Closed-Loop Retraining", 5)
    print("▶ Thu thập dữ liệu kiểm định trôi dạt phân phối (KS-Test & PSI)...")

    drift_detector = DriftDetector()
    ref_data, curr_data = drift_detector.generate_synthetic_drift_data(n_samples=100, inject_drift=True)
    report = drift_detector.detect_drift(ref_data, curr_data)

    print(f"{GREEN}✔ Kết quả kiểm định Data & Target Drift:{RESET}")
    print(f"  • Trạng thái trôi dạt tổng thể: {RED if report.get('drift_detected') else GREEN}{'CÓ TRÔI DẠT (DRIFT DETECTED)' if report.get('drift_detected') else 'ỔN ĐỊNH (NO DRIFT)'}{RESET}")
    print(f"  • Tỷ lệ đặc trưng bị trôi dạt:  {report.get('drift_share', 0) * 100:.1f}% (Ngưỡng cảnh báo: {report.get('drift_share_threshold', 0.3) * 100:.0f}%)")
    print(f"  • Số lượng đặc trưng kiểm định: {report.get('number_of_features')}")
    print(f"  • Chi tiết trôi dạt theo từng đặc trưng:")
    for col, d_info in list(report.get("features_evaluated", {}).items()):
        status_str = f"{RED}DRIFTED{RESET}" if d_info.get("drift_detected") else f"{GREEN}STABLE{RESET}"
        print(f"    - {col:25s}: p-value={d_info.get('p_value', 0):.4f} | PSI={d_info.get('psi', 0):.4f} -> [{status_str}]")

    print(f"\n{YELLOW}➔ Đây là dữ liệu drift tổng hợp để minh họa detector. Script này không gọi trigger_retraining.py, không huấn luyện, không đăng ký model và không reload Serving.{RESET}")


def step_6_plug_and_play_demo():
    print_banner("Minh họa Chuẩn hóa Payload Mẫu Trong Bộ Nhớ", 6)
    print("▶ Dữ liệu dưới đây là payload giả lập; chưa kết nối webhook Shopee hoặc nguồn thật:")

    real_webhook_payload = {
        "order_sn": "REAL_SHOPEE_20261001_9988",
        "_platform": "shopee",
        "item_sku": "ATN-CB-001",
        "item_name": "Áo thun nam cotton Premium Basic",
        "quantity": 2,
        "original_price": 129000,
        "total_order_value": 258000,
        "buyer_total_amount": 238000,
        "seller_discount": 20000,
        "shopee_discount": 0,
        "order_status": "READY_TO_SHIP",
        "shipping_carrier": "SPX Express",
        "state": "Hà Nội",
        "city": "Hà Nội",
        "district": "Cầu Giấy",
        "country": "VN",
        "create_time": datetime.now(timezone.utc).isoformat(),
    }

    print("  [Incoming Webhook] Nhận payload thật từ sàn TMĐT:")
    print(f"  JSON: {json.dumps(real_webhook_payload, indent=2, ensure_ascii=False)}")

    # 1. Chạy qua unify_schema
    df_real = pd.DataFrame([real_webhook_payload])
    unified_real = unify_schema(df_real)

    # 2. Chạy qua DataValidator
    validator = DataValidator()
    valid_df, invalid_df, val_report = validator.validate(unified_real)

    print(f"\n{GREEN}✔ KẾT QUẢ XỬ LÝ:{RESET}")
    print(f"  • Đã tiếp nhận và nhận diện đúng: Platform = {valid_df.iloc[0]['platform']}, Order ID = {valid_df.iloc[0]['order_id']}")
    print(f"  • Kiểm định chất lượng:           Pass Rate = {val_report.pass_rate * 100:.0f}% (Hợp lệ hoàn toàn)")
    print(f"  • Dữ liệu đã chuẩn hóa trong bộ nhớ; chưa ghi vào Kafka, MinIO hoặc Fact_Orders.")
    print(f"\n{BOLD}{YELLOW}★ Đây là demo logic từng module. Muốn trình diễn end-to-end cần chạy riêng các service và kiểm tra trạng thái /ready, model_source, history_source cùng dữ liệu xuất ra.{RESET}\n")


def main():
    print(f"\n{BOLD}{CYAN}╔════════════════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║     CHƯƠNG TRÌNH TRÌNH DIỄN LUỒNG HOẠT ĐỘNG STREAMING MLOPS E-COMMERCE     ║{RESET}")
    print(f"{BOLD}{CYAN}╚════════════════════════════════════════════════════════════════════════════╝{RESET}")

    events = step_1_streaming_simulation()
    step_2_unify_and_validate(events)
    step_3_feature_store_and_abc_xyz()
    step_4_model_serving_and_inventory()
    step_5_monitoring_and_retraining()
    step_6_plug_and_play_demo()


if __name__ == "__main__":
    main()
