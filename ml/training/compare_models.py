"""
compare_models.py
-----------------
Tổng hợp huấn luyện và đối chiếu toàn diện: Baseline (LightGBM / XGBoost) vs Deep Learning (LSTM / GRU):
  1. Đánh giá trên toàn bộ danh mục sản phẩm (Toàn hệ thống).
  2. Đánh giá phân rã theo ma trận 9 ô ABC/XYZ:
     - Phân khúc AX / AY (Doanh thu cao, ổn định / mùa vụ vừa phải)
     - Phân khúc AZ / BZ (Doanh thu cao/trung bình, biến động mạnh / Flash Sale)
     - Phân khúc C (Hàng đuôi dài / Long-tail)
  3. Trực quan hóa so sánh đa mô hình (WAPE bar chart, Segment heatmap, Overlay plot).
  4. Đăng ký kết quả và biểu đồ phân tích vào MLflow Tracking Server.

Tuần 6: So sánh Baseline vs Deep Learning & Lựa chọn mô hình Champion.
"""

import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ml.mlflow.setup_tracking import configure_mlflow
from ml.training.metrics import calculate_all_metrics, weighted_absolute_percentage_error
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from ml.training.train_baseline import (
    select_feature_columns,
    get_feature_data,
    get_daily_history_data,
    get_ml_model,
    SeasonalNaiveModel,
)
from ml.training.recursive_evaluation import forecast_fold_recursively
from ml.training.deep_learning_models import DeepLearningTrainer, HAS_TORCH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml.compare_models")


def evaluate_model_on_segments(
    preds: np.ndarray,
    actuals: np.ndarray,
    test_df: pd.DataFrame,
) -> Dict[str, float]:
    """Phân rã chỉ số WAPE theo từng nhóm phân khúc ABC/XYZ."""
    df_eval = test_df.copy().reset_index(drop=True)
    df_eval["actual"] = actuals[:len(df_eval)]
    df_eval["predicted"] = preds[:len(df_eval)]

    segment_wape = {}

    # 1. Nhóm AX / AY (Trụ cột ổn định)
    ax_ay = df_eval[df_eval["matrix_class"].isin(["AX", "AY"])]
    if len(ax_ay) > 0 and ax_ay["actual"].sum() > 0:
        segment_wape["WAPE_AX_AY"] = weighted_absolute_percentage_error(ax_ay["actual"].values, ax_ay["predicted"].values)
    else:
        segment_wape["WAPE_AX_AY"] = 0.0

    # 2. Nhóm AZ / BZ (Doanh số cao nhưng biến động mạnh)
    az_bz = df_eval[df_eval["matrix_class"].isin(["AZ", "BZ"])]
    if len(az_bz) > 0 and az_bz["actual"].sum() > 0:
        segment_wape["WAPE_AZ_BZ"] = weighted_absolute_percentage_error(az_bz["actual"].values, az_bz["predicted"].values)
    else:
        segment_wape["WAPE_AZ_BZ"] = 0.0

    # 3. Nhóm C (Hàng đuôi dài)
    c_group = df_eval[df_eval["abc_class"] == "C"]
    if len(c_group) > 0 and c_group["actual"].sum() > 0:
        segment_wape["WAPE_Group_C"] = weighted_absolute_percentage_error(c_group["actual"].values, c_group["predicted"].values)
    else:
        segment_wape["WAPE_Group_C"] = 0.0

    return segment_wape


def plot_comparison_visuals(
    summary_df: pd.DataFrame,
    artifacts_dir: str,
) -> List[str]:
    """Xuất các biểu đồ đối sánh trực quan giữa các mô hình."""
    saved_plots = []
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns

        os.makedirs(artifacts_dir, exist_ok=True)

        # 1. Biểu đồ thanh so sánh tổng quan WAPE giữa các mô hình
        plt.figure(figsize=(10, 5))
        colors = ["#2CA02C" if "LIGHTGBM" in m else "#1F77B4" if "LSTM" in m else "#7F7F7F" for m in summary_df["Mô hình"]]
        bars = plt.bar(summary_df["Mô hình"], summary_df["WAPE (%)"], color=colors, alpha=0.85, edgecolor="black")
        plt.title("So sánh Tổng thể Sai số WAPE (%) giữa các Mô hình (Càng thấp càng tốt)", fontsize=13, weight="bold")
        plt.ylabel("WAPE (%)")
        plt.ylim(0, max(summary_df["WAPE (%)"]) * 1.2)
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, yval + 1.0, f"{yval:.1f}%", ha="center", va="bottom", weight="bold")
        plt.tight_layout()

        p1 = os.path.join(artifacts_dir, "model_comparison_wape.png")
        plt.savefig(p1, dpi=120)
        plt.close()
        saved_plots.append(p1)

        # 2. Biểu đồ Heatmap đối chiếu hiệu năng theo từng phân khúc sản phẩm
        seg_cols = [c for c in ["WAPE_AX_AY", "WAPE_AZ_BZ", "WAPE_Group_C"] if c in summary_df.columns]
        if seg_cols:
            seg_df = summary_df.set_index("Mô hình")[seg_cols].rename(columns={
                "WAPE_AX_AY": "Nhóm AX/AY (Ổn định)",
                "WAPE_AZ_BZ": "Nhóm AZ/BZ (Biến động cao)",
                "WAPE_Group_C": "Nhóm C (Đuôi dài)",
            })
            plt.figure(figsize=(10, 5))
            sns.heatmap(seg_df, annot=True, fmt=".1f", cmap="YlOrRd_r", cbar=True, linewidths=0.5)
            plt.title("Đối chiếu Hiệu năng WAPE (%) theo Phân khúc Hàng hóa ABC/XYZ", fontsize=13, weight="bold")
            plt.tight_layout()

            p2 = os.path.join(artifacts_dir, "segment_comparison_heatmap.png")
            plt.savefig(p2, dpi=120)
            plt.close()
            saved_plots.append(p2)

    except Exception as e:
        logger.warning("Không thể xuất biểu đồ so sánh: %s", e)

    return saved_plots


def run_model_comparison(
    experiment_name: str = "demand-forecasting-model-comparison",
    n_splits: int = 3,
) -> pd.DataFrame:
    """
    Thực thi chu trình đối chiếu toàn diện Baseline vs Deep Learning.
    """
    logger.info("╔" + "═" * 68 + "╗")
    logger.info("║     ĐỐI CHIẾU TOÀN DIỆN: BASELINE (LIGHTGBM) vs DEEP LEARNING (LSTM) ║")
    logger.info("╚" + "═" * 68 + "╝")

    configure_mlflow(experiment_name=experiment_name)
    import mlflow

    df = get_feature_data()
    feature_cols, target_col = select_feature_columns(df)
    daily_history = get_daily_history_data(df)

    # Đọc siêu tham số đã tune nếu có
    params_file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "best_params_lightgbm.json")
    lgb_params = {"n_estimators": 140, "learning_rate": 0.05, "max_depth": 6, "num_leaves": 31}
    if os.path.exists(params_file):
        try:
            import json
            with open(params_file, "r") as f:
                lgb_params.update(json.load(f).get("params", {}))
            logger.info("✓ Đã nạp siêu tham số tối ưu cho LightGBM: %s", lgb_params)
        except Exception:
            pass

    # Danh mục các mô hình đối chiếu
    models_dict = {
        "Seasonal_Naive": SeasonalNaiveModel(),
        "Ridge_Regression": get_ml_model("ridge", {"alpha": 10.0}),
        "LightGBM_Tuned": get_ml_model("lightgbm", lgb_params),
        "LSTM_Network": DeepLearningTrainer(model_type="lstm", seq_len=14, hidden_dim=64, num_layers=2, max_epochs=25),
        "GRU_Network": DeepLearningTrainer(model_type="gru", seq_len=14, hidden_dim=64, num_layers=2, max_epochs=25),
    }

    artifacts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "comparison_artifacts"))
    os.makedirs(artifacts_dir, exist_ok=True)

    results = []

    # Chia fold theo ngày mà nhãn target_t_plus_1 đại diện.
    if "target_date" not in df.columns:
        df = df.copy()
        df["target_date"] = pd.to_datetime(df["date"]).dt.date + pd.Timedelta(days=1)
    splits = list(TimeSeriesFeatureExtractor.walk_forward_split(df, date_col="target_date", n_splits=n_splits))

    for m_name, model_obj in models_dict.items():
        logger.info("─" * 55)
        logger.info("Đánh giá mô hình: %s", m_name)

        fold_metrics_list = []
        seg_metrics_list = []
        all_actuals, all_preds = [], []

        for fold_idx, (train_df, val_df, test_df) in enumerate(splits, start=1):
            fit_df = pd.concat([train_df, val_df], ignore_index=True)

            X_train = fit_df[feature_cols].fillna(0).values
            y_train = fit_df[target_col].values

            # Huấn luyện
            if hasattr(model_obj, "fit"):
                if isinstance(model_obj, DeepLearningTrainer):
                    X_val_arr = val_df[feature_cols].fillna(0).values
                    y_val_arr = val_df[target_col].values
                    model_obj.fit(X_train, y_train, X_val=X_val_arr, y_val=y_val_arr)
                else:
                    # Sklearn / LightGBM
                    model_obj.fit(fit_df[feature_cols].fillna(0), y_train)

            # Dự báo test theo đúng cách serving chạy nhiều ngày: dự báo trước đó
            # được đưa vào lịch sử để dựng đặc trưng cho ngày sau.
            predict_one = None
            if isinstance(model_obj, DeepLearningTrainer):
                predict_one = lambda current, values, columns: current.predict(
                    np.asarray([values], dtype=np.float32)
                )
            scored_df, y_test, preds = forecast_fold_recursively(
                model=model_obj,
                feature_df=df,
                test_df=test_df,
                feature_cols=feature_cols,
                target_col=target_col,
                daily_history=daily_history,
                predict_one=predict_one,
            )
            if len(y_test) == 0:
                logger.warning("Fold %d không có SKU đủ lịch sử để đánh giá %s.", fold_idx, m_name)
                continue

            # Metrics tổng quát
            m = calculate_all_metrics(y_test, preds)
            fold_metrics_list.append(m)

            # Metrics theo phân khúc ABC/XYZ
            seg_m = evaluate_model_on_segments(preds, y_test, scored_df)
            seg_metrics_list.append(seg_m)

            all_actuals.extend(y_test)
            all_preds.extend(preds)

        # Trung bình metrics qua các folds
        overall_wape = round(float(np.mean([fm["wape"] for fm in fold_metrics_list])), 2)
        overall_mae = round(float(np.mean([fm["mae"] for fm in fold_metrics_list])), 2)
        overall_rmse = round(float(np.mean([fm["rmse"] for fm in fold_metrics_list])), 2)
        overall_r2 = round(float(np.mean([fm["r2"] for fm in fold_metrics_list])), 4)
        overall_bias = round(float(np.mean([fm["bias"] for fm in fold_metrics_list])), 2)

        seg_summary = {
            k: round(float(np.mean([sm[k] for sm in seg_metrics_list])), 2)
            for k in ["WAPE_AX_AY", "WAPE_AZ_BZ", "WAPE_Group_C"]
        }

        # Log vào MLflow
        try:
            with mlflow.start_run(run_name=f"compare_{m_name}"):
                mlflow.log_param("model_name", m_name)
                mlflow.log_param("evaluation_strategy", "recursive_multi_step")
                mlflow.log_metric("cv_wape", overall_wape)
                mlflow.log_metric("cv_mae", overall_mae)
                mlflow.log_metric("cv_rmse", overall_rmse)
                mlflow.log_metric("cv_r2", overall_r2)
                mlflow.log_metric("cv_bias", overall_bias)
                for sk, sv in seg_summary.items():
                    mlflow.log_metric(f"segment_{sk.lower()}", sv)
        except Exception:
            pass

        row_data = {
            "Mô hình": m_name,
            "Evaluation strategy": "recursive_multi_step",
            "WAPE (%)": overall_wape,
            "MAE": overall_mae,
            "RMSE": overall_rmse,
            "R2": overall_r2,
            "Bias (%)": overall_bias,
            **seg_summary,
        }
        results.append(row_data)

    comparison_df = pd.DataFrame(results).sort_values(by="WAPE (%)", ascending=True).reset_index(drop=True)

    # Xuất biểu đồ so sánh
    saved_plots = plot_comparison_visuals(comparison_df, artifacts_dir)

    # Lưu bảng so sánh ra CSV
    csv_path = os.path.join(artifacts_dir, "model_comparison_results.csv")
    comparison_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    logger.info("═" * 70)
    logger.info("  KẾT QUẢ SO SÁNH HIỆU NĂNG TỔNG HỢP VÀ PHÂN KHÚC ABC/XYZ")
    logger.info("═" * 70)
    print(comparison_df.to_string(index=False))
    logger.info("═" * 70)

    best_overall = comparison_df.iloc[0]["Mô hình"]
    logger.info("🏆 MÔ HÌNH CHIẾN THẮNG (CHAMPION MODEL): %s (WAPE = %.2f%%)", best_overall, comparison_df.iloc[0]["WAPE (%)"])

    return comparison_df


def main():
    parser = argparse.ArgumentParser(description="So sánh Baseline vs Deep Learning")
    parser.add_argument("--experiment-name", type=str, default="demand-forecasting-model-comparison")
    parser.add_argument("--n-splits", type=int, default=3)
    args = parser.parse_args()

    run_model_comparison(experiment_name=args.experiment_name, n_splits=args.n_splits)


if __name__ == "__main__":
    main()
