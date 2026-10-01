"""
data_validation.py
------------------
Kiểm định chất lượng dữ liệu (Data Quality & Validation) trước khi nạp vào Star Schema.

Bổ sung theo yêu cầu kiến trúc (khuyết điểm đã ghi nhận trong warehouse/etl/README.md):
  1. Kiểm tra tính toàn vẹn Schema (Schema Compliance).
  2. Kiểm tra giá trị bắt buộc (Null/Missing Checks trên khóa chính & ngoại).
  3. Kiểm tra miền giá trị hợp lệ (Domain & Range Checks: quantity > 0, price >= 0).
  4. Kiểm tra tính logic thời gian (Chronological Order: create <= pay <= ship <= complete).
  5. Phát hiện ngoại lai và cách ly bản ghi lỗi (Quarantine / Error Breakdown).
  6. Xuất báo cáo Data Quality Scorecard.

Tuần 3: Tầng Kiểm định Dữ liệu của pipeline ETL.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("etl.validation")


@dataclass
class ValidationReport:
    """Báo cáo tóm tắt chất lượng dữ liệu."""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    pass_rate: float = 0.0
    error_counts: Dict[str, int] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def print_summary(self):
        """In tóm tắt chất lượng dữ liệu ra logger."""
        logger.info("═" * 60)
        logger.info("  BÁO CÁO KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY)")
        logger.info("  Tổng số bản ghi:     %d", self.total_records)
        logger.info("  Bản ghi hợp lệ:      %d (%.2f%%)", self.valid_records, self.pass_rate * 100)
        logger.info("  Bản ghi bị cách ly:  %d", self.invalid_records)
        if self.error_counts:
            logger.info("  Chi tiết các lỗi phát hiện:")
            for err_type, count in self.error_counts.items():
                logger.info("    - %-35s: %d bản ghi", err_type, count)
        if self.warnings:
            logger.info("  Cảnh báo (Warnings):")
            for w in self.warnings:
                logger.info("    ! %s", w)
        logger.info("═" * 60)


class DataValidator:
    """Lớp kiểm định dữ liệu cho tập DataFrame đã qua unify_schema / clean_data."""

    # Các cột bắt buộc không được NULL
    REQUIRED_COLUMNS = ["order_id", "platform", "sku", "create_time", "order_status"]

    # Danh sách sàn hợp lệ
    VALID_PLATFORMS = {"shopee", "tiktok"}

    # Danh sách trạng thái hợp lệ
    VALID_STATUSES = {
        "UNPAID", "READY_TO_SHIP", "SHIPPED", "COMPLETED", "CANCELLED",
        "IN_CANCEL", "TO_RETURN", "AWAITING_SHIPMENT", "AWAITING_COLLECTION",
        "DELIVERED", "IN_TRANSIT",
    }

    def __init__(
        self,
        max_reasonable_quantity: int = 100,
        max_reasonable_amount: float = 1_000_000_000.0,
    ):
        self.max_qty = max_reasonable_quantity
        self.max_amount = max_reasonable_amount

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
        """
        Thực hiện kiểm định toàn diện trên DataFrame.

        Returns:
            Tuple[clean_df, quarantine_df, report]:
              - clean_df: các bản ghi đạt chuẩn sẵn sàng nạp vào Fact_Orders.
              - quarantine_df: các bản ghi không hợp lệ kèm cột 'rejection_reasons'.
              - report: đối tượng ValidationReport ghi lại số liệu thống kê.
        """
        report = ValidationReport(total_records=len(df))

        if df.empty:
            report.pass_rate = 1.0
            return df.copy(), pd.DataFrame(), report

        df_check = df.copy()
        errors_per_row: List[List[str]] = [[] for _ in range(len(df_check))]

        # 1. Kiểm tra Schema
        missing_cols = [c for c in self.REQUIRED_COLUMNS if c not in df_check.columns]
        if missing_cols:
            report.warnings.append(f"Thiếu các cột bắt buộc trong schema: {missing_cols}")
            for col in missing_cols:
                df_check[col] = None

        # Vectorized pre-parsing for timestamps to avoid calling pd.to_datetime per row
        ts_cols = ["create_time", "pay_time", "shipped_time", "completed_time", "cancel_time"]
        for c in ts_cols:
            if c in df_check.columns:
                df_check[c] = pd.to_datetime(df_check[c], errors="coerce")

        records = df_check.to_dict(orient="records")

        # 2. Kiểm tra từng bản ghi
        for idx, row in enumerate(records):
            row_errors = errors_per_row[idx]

            # Null checks
            for col in self.REQUIRED_COLUMNS:
                val = row.get(col)
                if pd.isna(val) or str(val).strip() == "":
                    row_errors.append(f"MISSING_{col.upper()}")

            # Platform domain check
            plat = str(row.get("platform", "")).lower()
            if plat not in self.VALID_PLATFORMS:
                row_errors.append("INVALID_PLATFORM")

            # Status domain check
            status = str(row.get("order_status", "")).upper()
            if status not in self.VALID_STATUSES:
                row_errors.append("INVALID_ORDER_STATUS")

            # Quantity check (> 0 và <= max_qty)
            qty = row.get("quantity")
            try:
                qty_val = float(qty)
                if pd.isna(qty_val) or qty_val <= 0:
                    row_errors.append("NON_POSITIVE_QUANTITY")
                elif qty_val > self.max_qty:
                    row_errors.append("OUTLIER_QUANTITY")
            except (ValueError, TypeError):
                row_errors.append("INVALID_QUANTITY_TYPE")

            # Price & Amount checks (không âm, không vượt trần phi lý)
            for amount_col in ["original_price", "subtotal", "buyer_total_amount"]:
                val = row.get(amount_col)
                if val is not None and not pd.isna(val):
                    try:
                        amt = float(val)
                        if amt < 0:
                            row_errors.append(f"NEGATIVE_{amount_col.upper()}")
                        elif amt > self.max_amount:
                            row_errors.append(f"OUTLIER_{amount_col.upper()}")
                    except (ValueError, TypeError):
                        row_errors.append(f"INVALID_{amount_col.upper()}_TYPE")

            # Chronological Order checks
            c_time = row.get("create_time")
            p_time = row.get("pay_time")
            s_time = row.get("shipped_time")
            d_time = row.get("completed_time")
            can_time = row.get("cancel_time")

            if pd.isna(c_time):
                row_errors.append("INVALID_CREATE_TIMESTAMP")
            else:
                if pd.notna(p_time) and p_time < c_time:
                    row_errors.append("PAY_TIME_BEFORE_CREATE_TIME")
                if pd.notna(s_time) and pd.notna(p_time) and s_time < p_time:
                    is_cod = str(row.get("is_cod", "false")).lower() in ("true", "1", "t")
                    if not is_cod:
                        row_errors.append("SHIP_TIME_BEFORE_PAY_TIME")
                if pd.notna(d_time) and pd.notna(s_time) and d_time < s_time:
                    row_errors.append("COMPLETE_TIME_BEFORE_SHIP_TIME")
                if pd.notna(can_time) and can_time < c_time:
                    row_errors.append("CANCEL_TIME_BEFORE_CREATE_TIME")

        # 3. Phân tách Clean vs Quarantine
        df_check["rejection_reasons"] = errors_per_row
        is_valid_mask = np.array([len(errs) == 0 for errs in errors_per_row], dtype=bool)

        clean_df = df_check[is_valid_mask].drop(columns=["rejection_reasons"]).copy()
        quarantine_df = df_check[~is_valid_mask].copy()
        quarantine_df["rejection_reasons"] = quarantine_df["rejection_reasons"].apply(lambda errs: "; ".join(errs))

        # 4. Thống kê kết quả
        report.valid_records = len(clean_df)
        report.invalid_records = len(quarantine_df)
        report.pass_rate = report.valid_records / report.total_records if report.total_records > 0 else 0.0

        # Đếm chi tiết lỗi
        error_map: Dict[str, int] = {}
        for err_list in errors_per_row:
            for err in err_list:
                error_map[err] = error_map.get(err, 0) + 1
        report.error_counts = error_map

        return clean_df, quarantine_df, report
