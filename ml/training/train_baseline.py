"""
train_baseline.py
-----------------
Huấn luyện và đánh giá các mô hình Baseline cho bài toán dự báo nhu cầu hàng hóa E-Commerce:
  1. Naive Baseline (Lag 1 - Nhu cầu hôm nay = hôm qua)
  2. Seasonal Naive Baseline (Lag 7 - Nhu cầu hôm nay = cùng thứ tuần trước)
  3. Moving Average Baseline (Trung bình trượt 7 ngày)
  4. Ridge Regression Baseline (Mô hình tuyến tính chính quy hóa L2)
  5. LightGBM / Gradient Boosting Regressor (Mô hình cây tăng áp độ dốc)

Quy trình MLOps:
  - Đánh giá bằng Walk-Forward Validation (Expanding Window, 3 folds) không rò rỉ dữ liệu.
  - Đo lường MAE, RMSE, MAPE, WAPE (tiêu chuẩn bán lẻ), Forecast Bias.
  - Tự động ghi nhận (Logging) toàn bộ tham số, metrics, biểu đồ thực tế vs dự báo và artifact mô hình lên MLflow Tracking Server.

Tuần 5: Huấn luyện Baseline & MLflow Experiment Tracking.
"""

import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

try:
    import numpy as np
    import pandas as pd
except ImportError:
    np = None
    pd = None

from dotenv import load_dotenv

load_dotenv()

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ml.mlflow.setup_tracking import configure_mlflow
from ml.training.metrics import calculate_all_metrics
from ml.features.time_series_features import TimeSeriesFeatureExtractor
from ml.features.feature_pipeline import load_orders_data, build_feature_store
from ml.training.recursive_evaluation import forecast_fold_recursively

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml.train_baseline")


def get_feature_data() -> pd.DataFrame:
    """Nạp Feature Store, tự động tạo mới nếu chưa tồn tại."""
    data_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "features_daily.parquet")
    if os.path.exists(data_path):
        logger.info("Nạp dữ liệu từ Feature Store: %s", data_path)
        df = pd.read_parquet(data_path)
        if df.empty:
            raise ValueError(
                "Feature Store đang rỗng; cần chạy lại feature pipeline với đủ lịch sử "
                "warehouse trước khi huấn luyện."
            )
        if "target_date" not in df.columns:
            df["target_date"] = pd.to_datetime(df["date"]).dt.date + pd.Timedelta(days=1)
        return df

    logger.info("Chưa có file Feature Store. Đang tự động chạy feature_pipeline...")
    orders_df = load_orders_data()
    df = build_feature_store(orders_df, save_path=data_path)
    df["target_date"] = pd.to_datetime(df["date"]).dt.date + pd.Timedelta(days=1)
    return df


def _daily_history_matches_feature(feature_df: pd.DataFrame, daily: pd.DataFrame) -> bool:
    expected = feature_df[["sku", "date", "daily_demand"]].copy()
    expected["date"] = pd.to_datetime(expected["date"]).dt.date
    actual = daily[["sku", "date", "daily_demand"]].copy()
    actual["date"] = pd.to_datetime(actual["date"]).dt.date
    aligned = expected.merge(
        actual,
        on=["sku", "date"],
        how="left",
        suffixes=("_feature", "_source"),
        validate="one_to_one",
    )
    return (
        len(aligned) == len(expected)
        and aligned["daily_demand_source"].notna().all()
        and np.allclose(
            aligned["daily_demand_feature"].to_numpy(dtype=float),
            aligned["daily_demand_source"].to_numpy(dtype=float),
        )
    )


def get_daily_history_data(feature_df: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    """Nạp chuỗi SKU × ngày đầy đủ để dựng đúng lịch sử lúc đánh giá đệ quy."""
    history_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "daily_history.parquet"))
    if os.path.exists(history_path):
        saved_history = pd.read_parquet(history_path)
        if feature_df is None or _daily_history_matches_feature(feature_df, saved_history):
            return saved_history
        logger.warning("daily_history.parquet không khớp Feature Store; thử khôi phục từ nguồn đơn hàng.")

    # Tương thích với Feature Store cũ chưa có daily_history.parquet: khôi phục từ
    # nguồn đơn hàng nếu khớp toàn bộ nhu cầu hiện tại trong Feature Store.
    if feature_df is not None:
        try:
            daily = TimeSeriesFeatureExtractor().aggregate_daily(load_orders_data())
            if _daily_history_matches_feature(feature_df, daily):
                daily.to_parquet(history_path, index=False)
                logger.info("Khôi phục daily_history.parquet từ nguồn đơn hàng khớp Feature Store.")
                return daily
        except Exception as exc:
            logger.warning("Không khôi phục được lịch sử đầy đủ từ nguồn đơn hàng: %s", exc)
    return None


def select_feature_columns(df: pd.DataFrame) -> Tuple[List[str], str]:
    """Lựa chọn các cột đặc trưng đầu vào (X) và biến mục tiêu (y)."""
    # Dùng chung định nghĩa với feature_pipeline để serving dựng đúng bộ cột này
    from ml.features.feature_pipeline import TARGET_COL, get_feature_columns

    target_col = TARGET_COL
    feature_cols = get_feature_columns(df)
    logger.info("Đã chọn %d đặc trưng huấn luyện: %s", len(feature_cols), feature_cols[:8])
    return feature_cols, target_col


# ─────────────────────────────────────────────────────────────
# CÁC MÔ HÌNH DỰ BÁO
# ─────────────────────────────────────────────────────────────

class NaiveModel:
    """Dự báo ngày mai = hôm nay (Lag 1)."""
    def __init__(self, lag_col: str = "daily_demand"):
        self.lag_col = lag_col

    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "lag_1" in X.columns:
            return X["lag_1"].fillna(0).values
        elif self.lag_col in X.columns:
            return X[self.lag_col].fillna(0).values
        return np.zeros(len(X))


class SeasonalNaiveModel:
    """Dự báo ngày mai = cùng thứ tuần trước (Lag 7)."""
    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "lag_7" in X.columns:
            return X["lag_7"].fillna(0).values
        return np.zeros(len(X))


class MovingAverageModel:
    """Dự báo ngày mai = trung bình trượt 7 ngày gần nhất."""
    def fit(self, X, y):
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if "rolling_mean_7" in X.columns:
            return X["rolling_mean_7"].fillna(0).values
        return np.zeros(len(X))


def get_ml_model(model_name: str, params: Optional[dict] = None):
    """Khởi tạo mô hình Machine Learning với thư viện khả dụng."""
    params = params or {}

    if model_name == "ridge":
        try:
            from sklearn.linear_model import Ridge
            return Ridge(alpha=params.get("alpha", 1.0))
        except ImportError:
            pass

    elif model_name == "lightgbm":
        try:
            import lightgbm as lgb
            return lgb.LGBMRegressor(
                n_estimators=params.get("n_estimators", 100),
                learning_rate=params.get("learning_rate", 0.05),
                max_depth=params.get("max_depth", 6),
                num_leaves=params.get("num_leaves", 31),
                random_state=42,
                verbosity=-1,
            )
        except ImportError:
            logger.warning("Thư viện LightGBM chưa được cài đặt, chuyển sang GradientBoostingRegressor của scikit-learn.")
            try:
                from sklearn.ensemble import GradientBoostingRegressor
                return GradientBoostingRegressor(
                    n_estimators=params.get("n_estimators", 80),
                    learning_rate=params.get("learning_rate", 0.05),
                    max_depth=params.get("max_depth", 5),
                    random_state=42,
                )
            except ImportError:
                pass

    elif model_name == "xgboost":
        try:
            import xgboost as xgb
            return xgb.XGBRegressor(
                n_estimators=params.get("n_estimators", 100),
                learning_rate=params.get("learning_rate", 0.05),
                max_depth=params.get("max_depth", 6),
                random_state=42,
                verbosity=0,
            )
        except ImportError:
            logger.warning("Thư viện XGBoost chưa được cài đặt, chuyển sang RandomForestRegressor.")
            try:
                from sklearn.ensemble import RandomForestRegressor
                return RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42)
            except ImportError:
                pass

    # Fallback mô hình đơn giản nếu không có scikit-learn
    return NaiveModel()


# ─────────────────────────────────────────────────────────────
# ĐÁNH GIÁ VÀ LOGGING LÊN MLFLOW
# ─────────────────────────────────────────────────────────────

def evaluate_walk_forward(
    model_name: str,
    model_obj,
    df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str,
    n_splits: int = 3,
    test_days: int = 14,
    val_days: int = 7,
    daily_history: Optional[pd.DataFrame] = None,
) -> Tuple[Dict[str, float], np.ndarray, np.ndarray]:
    """
    Đánh giá walk-forward theo ngày target bằng dự báo đệ quy hết từng test horizon.
    """
    if "target_date" not in df.columns:
        df = df.copy()
        df["target_date"] = pd.to_datetime(df["date"]).dt.date + pd.Timedelta(days=1)

    fold_metrics: List[Dict[str, float]] = []
    all_actuals = []
    all_preds = []

    split_gen = TimeSeriesFeatureExtractor.walk_forward_split(
        df, date_col="target_date", n_splits=n_splits, test_days=test_days, val_days=val_days
    )

    for fold_idx, (train_df, val_df, test_df) in enumerate(split_gen, start=1):
        # Kết hợp train + val để train mô hình cuối của fold
        fit_df = pd.concat([train_df, val_df], ignore_index=True)

        X_train = fit_df[feature_cols].fillna(0)
        y_train = fit_df[target_col].values

        # Fit trên các nhãn có target date trước test horizon.
        model_obj.fit(X_train, y_train)

        # Không dùng lag/rolling dựng sẵn trong test_df: các ngày test phải nhận
        # dự báo của ngày trước giống serving nhiều bước.
        scored_df, y_test, preds = forecast_fold_recursively(
            model=model_obj,
            feature_df=df,
            test_df=test_df,
            feature_cols=feature_cols,
            target_col=target_col,
            daily_history=daily_history,
        )
        if len(y_test) == 0:
            logger.warning("  [%s] Fold %d không có SKU đủ lịch sử để đánh giá.", model_name.upper(), fold_idx)
            continue

        metrics = calculate_all_metrics(y_test, preds)
        fold_metrics.append(metrics)
        all_actuals.extend(y_test)
        all_preds.extend(preds)

        logger.info(
            "  [%s] Fold %d/%d ➔ WAPE: %.2f%% | MAE: %.2f | RMSE: %.2f",
            model_name.upper(), fold_idx, n_splits, metrics["wape"], metrics["mae"], metrics["rmse"],
        )

    # Tính trung bình các folds
    avg_metrics: Dict[str, float] = {}
    for metric_key in ["wape", "mae", "rmse", "mape", "r2", "bias"]:
        vals = [fm[metric_key] for fm in fold_metrics]
        avg_metrics[metric_key] = (
            round(float(np.mean(vals)), 2 if "pe" in metric_key or metric_key == "bias" else 4)
            if vals else float("nan")
        )

    return avg_metrics, np.array(all_actuals), np.array(all_preds)


def plot_and_save_artifacts(
    model_name: str,
    actuals: np.ndarray,
    preds: np.ndarray,
    model_obj,
    feature_cols: List[str],
    artifacts_dir: str,
) -> List[str]:
    """Vẽ và lưu biểu đồ đánh giá Actual vs Predicted và Feature Importance."""
    saved_files = []
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns

        os.makedirs(artifacts_dir, exist_ok=True)

        # 1. Biểu đồ Actual vs Predicted (mẫu 60 điểm)
        sample_n = min(60, len(actuals))
        plt.figure(figsize=(12, 5))
        plt.plot(actuals[-sample_n:], label="Thực tế (Actual Demand)", color="#1F77B4", linewidth=2.0)
        plt.plot(preds[-sample_n:], label=f"Dự báo ({model_name})", color="#FF7F0E", linestyle="--", linewidth=2.0)
        plt.title(f"So sánh Nhu cầu Thực tế vs Dự báo — {model_name.upper()}")
        plt.xlabel("Mốc thời gian (Days)")
        plt.ylabel("Sản lượng (Quantity)")
        plt.legend()
        plt.tight_layout()

        pred_plot_path = os.path.join(artifacts_dir, f"{model_name}_actual_vs_predicted.png")
        plt.savefig(pred_plot_path, dpi=120)
        plt.close()
        saved_files.append(pred_plot_path)

        # 2. Biểu đồ Feature Importance (nếu mô hình có feature_importances_)
        if hasattr(model_obj, "feature_importances_"):
            importances = model_obj.feature_importances_
            top_indices = np.argsort(importances)[-15:]
            plt.figure(figsize=(10, 6))
            plt.barh(
                range(len(top_indices)),
                importances[top_indices],
                align="center",
                color="#2CA02C",
            )
            plt.yticks(range(len(top_indices)), [feature_cols[i] for i in top_indices])
            plt.title(f"Top 15 Đặc trưng Quan trọng Nhất — {model_name.upper()}")
            plt.xlabel("Tầm quan trọng (Feature Importance)")
            plt.tight_layout()

            fi_plot_path = os.path.join(artifacts_dir, f"{model_name}_feature_importance.png")
            plt.savefig(fi_plot_path, dpi=120)
            plt.close()
            saved_files.append(fi_plot_path)

    except Exception as e:
        logger.warning("Không thể xuất biểu đồ matplotlib: %s", e)

    return saved_files


def run_training_pipeline(
    experiment_name: str = "demand-forecasting-baseline",
    models_to_run: Optional[List[str]] = None,
    n_splits: int = 3,
) -> pd.DataFrame:
    """
    Chạy toàn bộ pipeline huấn luyện và log kết quả lên MLflow.
    """
    logger.info("╔" + "═" * 58 + "╗")
    logger.info("║         HUẤN LUYỆN BASELINE & EXPERIMENT TRACKING        ║")
    logger.info("╚" + "═" * 58 + "╝")

    # 1. Cấu hình MLflow
    tracking_uri = configure_mlflow(experiment_name=experiment_name)
    logger.info("MLflow Tracking URI: %s", tracking_uri)

    import mlflow

    # 2. Chuẩn bị dữ liệu
    df = get_feature_data()
    feature_cols, target_col = select_feature_columns(df)
    daily_history = get_daily_history_data(df)

    if models_to_run is None:
        models_to_run = ["naive", "seasonal_naive", "moving_average", "ridge", "lightgbm"]

    artifacts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "mlflow_artifacts"))

    results_table = []

    for m_name in models_to_run:
        logger.info("─" * 50)
        logger.info("Bắt đầu huấn luyện mô hình: %s", m_name.upper())

        # Khởi tạo đối tượng mô hình
        if m_name == "naive":
            model_obj = NaiveModel()
            params = {"method": "lag_1", "lags": [1]}
        elif m_name == "seasonal_naive":
            model_obj = SeasonalNaiveModel()
            params = {"method": "lag_7", "lags": [7]}
        elif m_name == "moving_average":
            model_obj = MovingAverageModel()
            params = {"method": "rolling_mean_7", "window": 7}
        elif m_name == "ridge":
            params = {"alpha": 10.0}
            model_obj = get_ml_model("ridge", params)
        elif m_name == "lightgbm":
            params = {"n_estimators": 120, "learning_rate": 0.05, "max_depth": 6, "num_leaves": 31}
            model_obj = get_ml_model("lightgbm", params)
        else:
            continue

        # Đánh giá bằng Walk-Forward Validation
        avg_metrics, actuals, preds = evaluate_walk_forward(
            model_name=m_name,
            model_obj=model_obj,
            df=df,
            feature_cols=feature_cols,
            target_col=target_col,
            n_splits=n_splits,
            daily_history=daily_history,
        )

        # Fit lại trên TOÀN BỘ dữ liệu trước khi lưu artifact phục vụ serving.
        # Sau walk-forward, model_obj đang giữ trọng số của fold cuối cùng — fold có cửa sổ
        # huấn luyện ngắn nhất (sớm nhất) → artifact cũ yếu hơn metric đã báo cáo.
        model_obj.fit(df[feature_cols].fillna(0), df[target_col].values)

        # Xuất biểu đồ artifacts
        saved_plots = plot_and_save_artifacts(
            model_name=m_name,
            actuals=actuals,
            preds=preds,
            model_obj=model_obj,
            feature_cols=feature_cols,
            artifacts_dir=artifacts_dir,
        )

        # Ghi log thí nghiệm lên MLflow
        try:
            with mlflow.start_run(run_name=f"baseline_{m_name}"):
                # Log Params
                mlflow.log_param("model_name", m_name)
                mlflow.log_param("n_features", len(feature_cols))
                mlflow.log_param("n_walk_forward_splits", n_splits)
                mlflow.log_param("evaluation_strategy", "recursive_multi_step")
                for pk, pv in params.items():
                    mlflow.log_param(pk, pv)

                # Log Metrics
                for mk, mv in avg_metrics.items():
                    mlflow.log_metric(f"cv_{mk}", mv)

                # Log Artifacts (Plots)
                for plot_file in saved_plots:
                    mlflow.log_artifact(plot_file, artifact_path="evaluation_plots")

                # Lưu mô hình (nếu có sklearn/joblib)
                try:
                    import joblib
                    model_path = os.path.join(artifacts_dir, f"{m_name}_model.joblib")
                    joblib.dump(model_obj, model_path)
                    mlflow.log_artifact(model_path, artifact_path="model")
                except Exception as e:
                    logger.warning("Không thể lưu artifact mô hình %s: %s", m_name, e)

                # Lưu hợp đồng đặc trưng cùng mô hình → serving dựng đúng cột/thứ tự/mã hóa
                spec_path = os.path.join(os.path.dirname(artifacts_dir), "feature_spec.json")
                if os.path.exists(spec_path):
                    mlflow.log_artifact(spec_path, artifact_path="model")

                logger.info("✓ Đã log thành công Run [%s] lên MLflow.", m_name)
        except Exception as e:
            logger.warning("Không thể log lên MLflow Tracking (%s), tiếp tục luồng...", e)

        results_table.append({
            "Mô hình": m_name.upper(),
            "WAPE (%)": avg_metrics["wape"],
            "MAE": avg_metrics["mae"],
            "RMSE": avg_metrics["rmse"],
            "MAPE (%)": avg_metrics["mape"],
            "R2": avg_metrics["r2"],
            "Bias (%)": avg_metrics["bias"],
        })

    summary_df = pd.DataFrame(results_table).sort_values(by="WAPE (%)", ascending=True).reset_index(drop=True)

    logger.info("═" * 60)
    logger.info("  BẢNG TỔNG HỢP SO SÁNH HIỆU NĂNG CÁC MÔ HÌNH BASELINE")
    logger.info("═" * 60)
    for idx, row in summary_df.iterrows():
        logger.info(
            "  #%d %-18s | WAPE: %6.2f%% | MAE: %6.2f | RMSE: %6.2f | R2: %5.2f | Bias: %6.2f%%",
            idx + 1, row["Mô hình"], row["WAPE (%)"], row["MAE"], row["RMSE"], row["R2"], row["Bias (%)"]
        )
    logger.info("═" * 60)

    best_model = summary_df.iloc[0]["Mô hình"]
    logger.info("🏆 MÔ HÌNH TỐI ƯU NHẤT TRÊN TEST SPLITS: %s (WAPE = %.2f%%)", best_model, summary_df.iloc[0]["WAPE (%)"])

    return summary_df


def main():
    parser = argparse.ArgumentParser(description="Huấn luyện mô hình Baseline và Log lên MLflow")
    parser.add_argument("--experiment-name", type=str, default="demand-forecasting-baseline")
    parser.add_argument("--n-splits", type=int, default=3)
    parser.add_argument("--models", nargs="+", default=["naive", "seasonal_naive", "moving_average", "ridge", "lightgbm"])
    args = parser.parse_args()

    run_training_pipeline(
        experiment_name=args.experiment_name,
        models_to_run=args.models,
        n_splits=args.n_splits,
    )


if __name__ == "__main__":
    main()
