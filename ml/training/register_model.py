"""
register_model.py
-----------------
Tự động hóa đăng ký mô hình chiến thắng (Champion Model) vào MLflow Model Registry:
  1. Quét các runs trong MLflow Tracking Server để tìm mô hình có chỉ số WAPE thấp nhất.
  2. Đăng ký mô hình vào MLflow Model Registry với tên: `ECommerceDemandForecastModel`.
  3. Gán thẻ tags và mô tả phiên bản (Framework, Metrics, Target, Input Features).
  4. Chuyển trạng thái mô hình sang `Staging` (sẵn sàng phục vụ cho FastAPI Model Serving ở Tuần 7).
  5. Xuất bản manifest phục vụ triển khai tại: `data/model_manifest.json`.

Tuần 6: Đăng ký và quản lý vòng đời mô hình trên MLflow Model Registry.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Cho phép import từ thư mục gốc
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ml.mlflow.setup_tracking import configure_mlflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mlops.registry")


REGISTERED_MODEL_NAME = "ECommerceDemandForecastModel"


def find_best_run(experiment_name: str) -> Optional[Dict[str, Any]]:
    """Tìm Run có WAPE thấp nhất từ MLflow Tracking Server."""
    try:
        import mlflow
        exp = mlflow.get_experiment_by_name(experiment_name)
        if exp is None:
            logger.warning("Không tìm thấy experiment '%s' trên MLflow.", experiment_name)
            return None

        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["metrics.cv_wape ASC"],
        )

        if runs.empty:
            logger.warning("Experiment '%s' chưa có run nào.", experiment_name)
            return None

        best_run = runs.iloc[0]
        model_name = best_run.get("params.model_name", "lightgbm")
        return {
            "run_id": best_run["run_id"],
            "model_name": model_name,
            "wape": best_run.get("metrics.cv_wape"),
            "mae": best_run.get("metrics.cv_mae"),
            "rmse": best_run.get("metrics.cv_rmse"),
            "artifact_uri": best_run.get("artifact_uri", ""),
            "model_file": f"{str(model_name).lower()}_model.joblib",
        }
    except Exception as e:
        logger.warning("Không thể truy vấn MLflow Server (%s), chuyển sang đọc bảng so sánh cục bộ...", e)

    # Fallback: đọc từ file CSV so sánh cục bộ nếu có
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "comparison_artifacts", "model_comparison_results.csv")
    if os.path.exists(csv_path):
        import pandas as pd
        comp_df = pd.read_csv(csv_path)
        top = comp_df.iloc[0]
        return {
            "run_id": "local_champion_run",
            "model_name": top["Mô hình"],
            "wape": top["WAPE (%)"],
            "mae": top["MAE"],
            "rmse": top["RMSE"],
            "artifact_uri": "data/champion_model.joblib",
            "model_file": f"{str(top['Mô hình']).lower()}_model.joblib",
        }

    # Không còn trả về metrics "mặc định" (24.5 / 1.85 / 2.60): không có run thật thì
    # không có gì để đăng ký — tránh manifest chứa số liệu bịa.
    return None


def _feature_count() -> int:
    """Số đặc trưng đầu vào thực tế, đọc từ data/feature_spec.json (không hardcode)."""
    spec_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "feature_spec.json")
    try:
        with open(spec_path, "r", encoding="utf-8") as f:
            return len(json.load(f)["feature_columns"])
    except Exception:
        return 0


def register_champion_model(
    experiment_name: str = "demand-forecasting-model-comparison",
    model_name: str = REGISTERED_MODEL_NAME,
    stage: str = "Staging",
    manifest_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Đăng ký mô hình Champion vào Model Registry và xuất manifest.
    """
    logger.info("═" * 60)
    logger.info("  BẮT ĐẦU QUY TRÌNH ĐĂNG KÝ MÔ HÌNH VÀO MLFLOW MODEL REGISTRY")
    logger.info("  Tên mô hình: %s | Mục tiêu Stage: %s", model_name, stage)
    logger.info("═" * 60)

    configure_mlflow(experiment_name=experiment_name)
    best_info = find_best_run(experiment_name)

    if not best_info:
        logger.error("Không tìm thấy thông tin mô hình Champion.")
        return {}

    logger.info("🏆 Mô hình Champion được lựa chọn: %s (WAPE: %.2f%%, Run ID: %s)",
                best_info["model_name"], best_info["wape"], best_info["run_id"])

    # 1. Đăng ký vào MLflow Model Registry nếu server khả dụng
    registered_version = "1"
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        client = MlflowClient()

        # Tạo registered model nếu chưa tồn tại
        try:
            client.create_registered_model(
                name=model_name,
                description="Mô hình dự báo nhu cầu hàng hóa E-Commerce cho hệ thống bán lẻ đa kênh (Shopee + TikTok Shop).",
                tags={"project": "streaming-mlops-ecommerce", "task": "demand_forecasting"},
            )
            logger.info("✓ Đã tạo Registered Model mới: %s", model_name)
        except Exception:
            logger.info("Registered Model '%s' đã tồn tại trong Registry.", model_name)

        # Đăng ký version mới từ run artifact
        if best_info["run_id"] != "local_champion_run" and best_info["run_id"] != "default_champion_run":
            model_uri = f"runs:/{best_info['run_id']}/model"
            model_version = client.create_model_version(
                name=model_name,
                source=model_uri,
                run_id=best_info["run_id"],
                description=f"Champion model {best_info['model_name']} với CV WAPE = {best_info['wape']}%.",
                tags={"framework": str(best_info["model_name"]), "wape": str(best_info["wape"])},
            )
            registered_version = str(model_version.version)
            logger.info("✓ Đã tạo phiên bản: Version %s", registered_version)

            # Chuyển trạng thái sang Staging
            client.transition_model_version_stage(
                name=model_name,
                version=registered_version,
                stage=stage,
                archive_existing_versions=True,
            )
            logger.info("✓ Đã chuyển Version %s sang trạng thái: %s", registered_version, stage)

    except Exception as e:
        logger.warning("Không thể đăng ký trực tiếp qua MLflow Client (%s), ghi nhận Manifest...", e)

    # 2. Tạo Model Manifest JSON phục vụ trực tiếp cho FastAPI Serving (Tuần 7)
    manifest = {
        "model_registry_name": model_name,
        "version": registered_version,
        "stage": stage,
        "champion_algorithm": str(best_info["model_name"]),
        "metrics": {
            k: float(best_info[src])
            for k, src in (("cv_wape", "wape"), ("cv_mae", "mae"), ("cv_rmse", "rmse"))
            if best_info.get(src) is not None
        },
        "run_id": best_info["run_id"],
        "model_file": best_info.get("model_file"),
        "target_variable": "daily_demand_t_plus_1",
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "input_feature_count": _feature_count(),
        "lead_time_days": 3,
        "service_level": 0.95,
        "status": "READY_FOR_SERVING",
    }

    manifest_dir = os.path.abspath(
        manifest_dir or os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, "model_manifest.json")

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    logger.info("✓ Đã xuất Model Manifest tại: %s", manifest_path)
    logger.info("═" * 60)
    logger.info("  HOÀN THÀNH QUY TRÌNH ĐĂNG KÝ MÔ HÌNH VÀO REGISTRY")
    logger.info("═" * 60)

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Đăng ký Champion Model vào MLflow Registry")
    parser.add_argument("--stage", type=str, default="Staging", choices=["Staging", "Production"])
    parser.add_argument("--experiment-name", type=str, default="demand-forecasting-model-comparison")
    args = parser.parse_args()

    register_champion_model(experiment_name=args.experiment_name, stage=args.stage)


if __name__ == "__main__":
    main()
