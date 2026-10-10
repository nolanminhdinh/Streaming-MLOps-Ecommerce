"""
abc_xyz.py
----------
Module phân loại danh mục hàng hóa theo ma trận ABC/XYZ kết hợp chính sách tồn kho:
  1. Phân loại ABC (Pareto Analysis):
     - Dựa trên doanh thu đóng góp tích lũy của từng SKU.
     - Nhóm A: Top sản phẩm tạo ra 80% tổng doanh thu (thường chiếm ~20% SKU).
     - Nhóm B: Các sản phẩm tạo ra 15% doanh thu tiếp theo (chiếm ~30% SKU).
     - Nhóm C: Các sản phẩm chỉ tạo ra 5% doanh thu còn lại (chiếm ~50% SKU đuôi dài - Long tail).

  2. Phân loại XYZ (Demand Variability Analysis):
     - Dựa trên hệ số biến thiên nhu cầu (Coefficient of Variation: CV = std / mean).
     - Nhóm X (CV <= 0.5): Nhu cầu rất ổn định, tính dự báo rất cao.
     - Nhóm Y (0.5 < CV <= 1.0): Nhu cầu biến động vừa phải (theo mùa vụ/khuyến mãi).
     - Nhóm Z (CV > 1.0): Nhu cầu biến động dữ dội, gián đoạn (lumpy demand), khó dự báo.

  3. Ma trận 9 ô ABC/XYZ (9-Box Matrix):
     - Xác định chiến lược tồn kho (Safety Stock, Reorder Point).
     - Lựa chọn mô hình Machine Learning phù hợp cho từng phân khúc SKU.

Tuần 4: Module cốt lõi tiền xử lý và phân loại hàng hóa.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("ml.abc_xyz")


@dataclass
class InventoryPolicy:
    """Chính sách tồn kho tính toán cho từng SKU."""
    sku: str
    abc_class: str
    xyz_class: str
    matrix_class: str  # AX, AY, AZ, BX, BY, BZ, CX, CY, CZ
    avg_daily_demand: float
    std_daily_demand: float
    cv: float
    safety_stock: int
    reorder_point: int
    service_level: float
    strategy_recommendation: str
    recommended_model: str


class ABCXYZClassifier:
    """Lớp thực hiện phân loại ma trận ABC/XYZ và hoạch định tồn kho."""

    def __init__(
        self,
        pareto_a: float = 0.80,
        pareto_b: float = 0.95,
        cv_x: float = 0.50,
        cv_y: float = 1.00,
        default_lead_time_days: int = 3,
        default_service_level: float = 0.95,
    ):
        """
        Args:
            pareto_a: Ngưỡng doanh thu tích lũy nhóm A (mặc định 80%).
            pareto_b: Ngưỡng doanh thu tích lũy nhóm B (mặc định 95%).
            cv_x: Ngưỡng CV nhóm X (mặc định 0.50).
            cv_y: Ngưỡng CV nhóm Y (mặc định 1.00).
            default_lead_time_days: Thời gian giao hàng trung bình của nhà cung cấp (ngày).
            default_service_level: Mức độ đáp ứng dịch vụ mong muốn (mặc định 95% -> Z ~ 1.65).
        """
        self.pareto_a = pareto_a
        self.pareto_b = pareto_b
        self.cv_x = cv_x
        self.cv_y = cv_y
        self.lead_time = default_lead_time_days
        self.service_level = default_service_level

        # Z-score tương ứng với Service Level chuẩn hóa
        self._z_scores = {
            0.90: 1.28,
            0.95: 1.65,
            0.98: 2.05,
            0.99: 2.33,
        }

    def _get_z_score(self, service_level: float) -> float:
        """Lấy giá trị Z-score từ phân phối chuẩn theo Service Level."""
        if service_level in self._z_scores:
            return self._z_scores[service_level]
        # Xấp xỉ đơn giản nếu giá trị khác bảng
        if service_level >= 0.99:
            return 2.33
        elif service_level >= 0.95:
            return 1.65
        elif service_level >= 0.90:
            return 1.28
        return 1.0

    def fit_transform(
        self,
        orders_df: pd.DataFrame,
        sku_col: str = "sku",
        date_col: str = "create_time",
        quantity_col: str = "quantity",
        revenue_col: str = "buyer_total_amount",
    ) -> pd.DataFrame:
        """
        Thực hiện toàn bộ quy trình phân loại ABC/XYZ từ dữ liệu đơn hàng.

        Args:
            orders_df: DataFrame đơn hàng (đã làm sạch, loại bỏ đơn hủy).
            sku_col: Tên cột SKU sản phẩm.
            date_col: Tên cột thời gian tạo đơn.
            quantity_col: Tên cột số lượng bán.
            revenue_col: Tên cột doanh thu thực tế.

        Returns:
            pd.DataFrame tổng hợp kết quả phân loại và chính sách tồn kho theo từng SKU.
        """
        df = orders_df.copy()

        # Loại bỏ các đơn bị hủy nếu có cột is_cancelled
        if "is_cancelled" in df.columns:
            df = df[~df["is_cancelled"]].copy()
        if "order_status" in df.columns:
            df = df[df["order_status"] != "CANCELLED"].copy()

        # Chuẩn hóa cột ngày
        df["order_date"] = pd.to_datetime(df[date_col]).dt.date

        # ─────────────────────────────────────────────────────
        # 1. PHÂN LOẠI ABC: Tính tổng doanh thu theo SKU
        # ─────────────────────────────────────────────────────
        sku_revenue = (
            df.groupby(sku_col)[revenue_col]
            .sum()
            .reset_index()
            .rename(columns={revenue_col: "total_revenue"})
            .sort_values(by="total_revenue", ascending=False)
        )

        total_rev_all = sku_revenue["total_revenue"].sum()
        if not np.isfinite(total_rev_all) or total_rev_all <= 0:
            # Historical sources may omit prices or mix currencies. In that
            # case classify ABC by units sold instead of assigning every SKU
            # to A from an all-zero revenue series.
            logger.warning(
                "Không có doanh thu dương để phân loại ABC; chuyển sang tổng số lượng bán theo SKU."
            )
            sku_revenue = (
                df.groupby(sku_col)[quantity_col]
                .sum()
                .reset_index()
                .rename(columns={quantity_col: "total_revenue"})
                .sort_values(by="total_revenue", ascending=False)
            )
            total_rev_all = sku_revenue["total_revenue"].sum()

        if total_rev_all > 0:
            sku_revenue["rev_share"] = sku_revenue["total_revenue"] / total_rev_all
            sku_revenue["cum_rev_share"] = sku_revenue["rev_share"].cumsum()
        else:
            sku_revenue["rev_share"] = 0.0
            sku_revenue["cum_rev_share"] = 0.0

        # Phân loại ABC: dùng ngưỡng tích lũy của phần tử trước đó (hoặc phần tử đầu tiên luôn là A)
        sku_revenue["prev_cum_share"] = sku_revenue["cum_rev_share"].shift(1, fill_value=0.0)

        def assign_abc(row: pd.Series) -> str:
            # Nếu phần tử trước đó < pareto_a thì phần tử này thuộc nhóm A (bao gồm SKU đầu tiên)
            if row["prev_cum_share"] < self.pareto_a:
                return "A"
            elif row["prev_cum_share"] < self.pareto_b:
                return "B"
            else:
                return "C"

        sku_revenue["abc_class"] = sku_revenue.apply(assign_abc, axis=1)
        sku_revenue.drop(columns=["prev_cum_share"], inplace=True)

        # ─────────────────────────────────────────────────────
        # 2. PHÂN LOẠI XYZ: Tính biến động nhu cầu hàng ngày
        # ─────────────────────────────────────────────────────
        # Tạo chuỗi thời gian đầy đủ cho từng SKU để không bỏ sót ngày bán bằng 0
        min_date = df["order_date"].min()
        max_date = df["order_date"].max()
        all_dates = pd.date_range(min_date, max_date, freq="D").date

        unique_skus = df[sku_col].unique()
        idx = pd.MultiIndex.from_product([unique_skus, all_dates], names=[sku_col, "order_date"])
        grid_df = pd.DataFrame(index=idx).reset_index()

        daily_sku_qty = (
            df.groupby([sku_col, "order_date"])[quantity_col]
            .sum()
            .reset_index()
        )

        # Merge để lấp đầy ngày không có đơn với quantity = 0
        full_daily = pd.merge(grid_df, daily_sku_qty, on=[sku_col, "order_date"], how="left")
        full_daily[quantity_col] = full_daily[quantity_col].fillna(0)

        # Thống kê mean, std, CV theo từng SKU
        sku_stats = (
            full_daily.groupby(sku_col)[quantity_col]
            .agg(
                avg_daily_demand="mean",
                std_daily_demand="std",
                total_quantity="sum",
            )
            .reset_index()
        )

        # Tránh chia cho 0
        sku_stats["cv"] = np.where(
            sku_stats["avg_daily_demand"] > 0,
            sku_stats["std_daily_demand"] / sku_stats["avg_daily_demand"],
            999.0,
        )

        def assign_xyz(cv: float) -> str:
            if cv <= self.cv_x:
                return "X"
            elif cv <= self.cv_y:
                return "Y"
            else:
                return "Z"

        sku_stats["xyz_class"] = sku_stats["cv"].apply(assign_xyz)

        # ─────────────────────────────────────────────────────
        # 3. KẾT HỢP MA TRẬN 9 Ô ABC/XYZ & HOẠCH ĐỊNH TỒN KHO
        # ─────────────────────────────────────────────────────
        result = pd.merge(sku_revenue, sku_stats, on=sku_col, how="inner")
        result["matrix_class"] = result["abc_class"] + result["xyz_class"]

        # Lấy tên sản phẩm và category nếu có trong orders_df
        meta_cols = [c for c in ["product_name", "category", "unit_cost"] if c in df.columns]
        if meta_cols:
            sku_meta = df[[sku_col] + meta_cols].drop_duplicates(subset=[sku_col])
            result = pd.merge(result, sku_meta, on=sku_col, how="left")

        # Tính toán Safety Stock (SS) và Reorder Point (ROP)
        # Công thức:
        #   SS  = Z * sigma_d * sqrt(Lead_Time)
        #   ROP = (d_avg * Lead_Time) + SS
        z = self._get_z_score(self.service_level)
        lt_sqrt = np.sqrt(self.lead_time)

        result["safety_stock"] = np.ceil(z * result["std_daily_demand"] * lt_sqrt).astype(int)
        result["reorder_point"] = np.ceil(
            (result["avg_daily_demand"] * self.lead_time) + result["safety_stock"]
        ).astype(int)

        # Gán khuyến nghị chiến lược và mô hình dự báo phù hợp
        strategies = {
            "AX": ("Chiến lược Just-In-Time (JIT), kiểm soát tồn kho chặt chẽ, tối thiểu chi phí lưu kho.",
                   "LightGBM / ARIMA / Exponential Smoothing (Độ chính xác rất cao)"),
            "AY": ("Giám sát tồn kho định kỳ, chuẩn bị đệm trước sự kiện khuyến mãi / Flash Sale.",
                   "XGBoost / LightGBM tích hợp feature khuyến mãi, ngày đôi"),
            "AZ": ("Sản phẩm doanh thu cao nhưng nhu cầu biến động mạnh; cần lượng Safety Stock dự phòng cao.",
                   "Deep Learning (LSTM/GRU) hoặc Mô hình tổng hợp Ensemble"),
            "BX": ("Áp dụng điểm đặt hàng lại tự động (Automated ROP), kiểm soát định kỳ vừa phải.",
                   "LightGBM Baseline / Random Forest"),
            "BY": ("Dự báo theo chu kỳ mùa vụ / Flash sale, tối ưu lô đặt hàng kinh tế (EOQ).",
                   "LightGBM / Prophet với đặc trưng chuỗi thời gian"),
            "BZ": ("Tồn kho theo lô nhỏ, cảnh giác nguy cơ tồn đọng vốn giá trị trung bình.",
                   "XGBoost kết hợp phân loại xác suất phát sinh nhu cầu (Two-stage)"),
            "CX": ("Giá trị thấp, nhu cầu đều; áp dụng điểm đặt hàng tự động với lô lớn để giảm chi phí đặt hàng.",
                   "Heuristic trung bình trượt / Moving Average"),
            "CY": ("Giá trị thấp, nhu cầu biến động; duy trì tồn kho tối thiểu, đặt hàng theo lô.",
                   "Mô hình giản đơn hoặc phân tích mùa vụ đơn giản"),
            "CZ": ("Hàng đuôi dài rủi ro tồn kho cao; cân nhắc Make-to-Order hoặc Dropshipping.",
                   "Phát hiện nhu cầu gián đoạn (Croston's Method) hoặc tồn kho tối thiểu"),
        }

        result["strategy_recommendation"] = result["matrix_class"].apply(
            lambda k: strategies.get(k, ("Duy trì chính sách chuẩn", "Baseline ML"))[0]
        )
        result["recommended_model"] = result["matrix_class"].apply(
            lambda k: strategies.get(k, ("Duy trì chính sách chuẩn", "Baseline ML"))[1]
        )

        logger.info(
            "Phân loại ABC/XYZ hoàn tất cho %d SKUs. Phân bố: %s",
            len(result), result["matrix_class"].value_counts().to_dict(),
        )

        return result

    def get_summary_report(self, classification_df: pd.DataFrame) -> dict:
        """Xuất báo cáo thống kê tóm tắt theo ma trận ABC/XYZ."""
        df = classification_df
        total_skus = len(df)
        total_rev = df["total_revenue"].sum()

        summary = {
            "total_skus": total_skus,
            "total_revenue": total_rev,
            "abc_breakdown": {},
            "xyz_breakdown": {},
            "matrix_breakdown": {},
        }

        for cat in ["A", "B", "C"]:
            sub = df[df["abc_class"] == cat]
            summary["abc_breakdown"][cat] = {
                "sku_count": len(sub),
                "sku_pct": round(len(sub) / total_skus * 100, 2) if total_skus > 0 else 0,
                "revenue": sub["total_revenue"].sum(),
                "revenue_pct": round(sub["total_revenue"].sum() / total_rev * 100, 2) if total_rev > 0 else 0,
            }

        for cat in ["X", "Y", "Z"]:
            sub = df[df["xyz_class"] == cat]
            summary["xyz_breakdown"][cat] = {
                "sku_count": len(sub),
                "sku_pct": round(len(sub) / total_skus * 100, 2) if total_skus > 0 else 0,
                "avg_cv": round(sub["cv"].mean(), 2) if len(sub) > 0 else 0,
            }

        for m in ["AX", "AY", "AZ", "BX", "BY", "BZ", "CX", "CY", "CZ"]:
            sub = df[df["matrix_class"] == m]
            summary["matrix_breakdown"][m] = {
                "sku_count": len(sub),
                "sku_pct": round(len(sub) / total_skus * 100, 2) if total_skus > 0 else 0,
                "revenue_pct": round(sub["total_revenue"].sum() / total_rev * 100, 2) if total_rev > 0 else 0,
            }

        return summary
