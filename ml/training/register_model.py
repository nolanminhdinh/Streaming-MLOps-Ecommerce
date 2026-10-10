"""
register_model.py
-----------------
Tự động hóa đăng ký mô hình chiến thắng (Champion Model) vào MLflow Model Registry:
  1. Quét các runs đã đánh giá đệ quy trên nhiều ngày để tìm mô hình có WAPE thấp nhất.
  2. Đăng ký mô hình vào MLflow Model Registry với tên: `ECommerceDemandForecastModel`.
  3. Gán thẻ tags và mô tả phiên bản (Framework, Metrics, Target, Input Features).
  4. Chuyển trạng thái mô hình sang `Staging` (sẵn sàng phục vụ cho FastAPI Model Serving ở Tuần 7).
  5. Xuất bản manifest phục vụ triển khai tại: `data/model_manifest.json`.

Tuần 6: Đăng ký và quản lý vòng đời mô hình trên MLflow Model Registry.
"""

import argparse
import json
import logging
import math
import os
import sys
import time
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
REQUIRED_EVALUATION_STRATEGY = "recursive_multi_step"


def find_best_run(
    experiment_name: str,
    required_data_origin: Optional[str] = None,
    required_source_datasets: Optional[list[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Tìm Run có WAPE thấp nhất theo giao thức đánh giá đệ quy hiện tại."""
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

        strategy_col = "params.evaluation_strategy"
        if strategy_col not in runs.columns:
            logger.warning(
                "Experiment '%s' chưa có run dùng đánh giá '%s'; chưa thể chọn champion.",
                experiment_name,
                REQUIRED_EVALUATION_STRATEGY,
            )
            return None
        compatible_runs = runs[runs[strategy_col] == REQUIRED_EVALUATION_STRATEGY]
        if compatible_runs.empty:
            logger.warning(
                "Experiment '%s' chưa có run dùng đánh giá '%s'; chưa thể chọn champion.",
                experiment_name,
                REQUIRED_EVALUATION_STRATEGY,
            )
            return None
        runs = compatible_runs

        if required_data_origin:
            origin_col = "params.data_origin"
            if origin_col not in runs.columns:
                logger.warning("Experiment chưa có run ghi data_origin=%s.", required_data_origin)
                return None
            runs = runs[runs[origin_col] == required_data_origin]
        if required_source_datasets is not None:
            import json
            datasets_col = "params.source_datasets"
            expected_datasets = json.dumps(sorted(required_source_datasets), separators=(",", ":"))
            if datasets_col not in runs.columns:
                logger.warning("Experiment chưa có run ghi fingerprint source_datasets hiện hành.")
                return None
            runs = runs[runs[datasets_col] == expected_datasets]

        if runs.empty:
            logger.warning(
                "Experiment '%s' không có run khớp evaluation strategy và nguồn dữ liệu hiện hành.",
                experiment_name,
            )
            return None

        from mlflow.tracking import MlflowClient
        client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())
        for _, candidate in runs.iterrows():
            model_name = candidate.get("params.model_name", "lightgbm")
            model_file = f"{str(model_name).lower()}_model.joblib"
            run_id = candidate["run_id"]
            try:
                artifacts = client.list_artifacts(run_id, path="model")
                artifact_names = {os.path.basename(item.path) for item in artifacts if not item.is_dir}
            except Exception as artifact_error:
                logger.warning("Bỏ run %s vì không đọc được model artifacts (%s).", run_id, artifact_error)
                continue
            if model_file not in artifact_names or "feature_spec.json" not in artifact_names:
                logger.warning("Bỏ run %s vì thiếu model artifact hoặc feature_spec.json.", run_id)
                continue
            try:
                import joblib
                model_dir = mlflow.artifacts.download_artifacts(
                    artifact_uri=f"runs:/{run_id}/model",
                    tracking_uri=mlflow.get_tracking_uri(),
                )
                loaded_model = joblib.load(os.path.join(model_dir, model_file))
                if not hasattr(loaded_model, "predict"):
                    raise TypeError("Artifact thiếu phương thức predict.")
                del loaded_model
            except Exception as artifact_error:
                logger.warning("Bỏ run %s vì model artifact không nạp được (%s).", run_id, artifact_error)
                continue
            return {
                "run_id": run_id,
                "model_name": model_name,
                "wape": candidate.get("metrics.cv_wape"),
                "mae": candidate.get("metrics.cv_mae"),
                "rmse": candidate.get("metrics.cv_rmse"),
                "artifact_uri": candidate.get("artifact_uri", ""),
                "model_file": model_file,
            }
        logger.warning("Không có run khớp dữ liệu hiện hành nào lưu đủ model artifact và feature spec.")
        return None
    except Exception as e:
        logger.warning("Không thể truy vấn MLflow Server (%s), chuyển sang đọc bảng so sánh cục bộ...", e)

    # Fallback: đọc từ file CSV so sánh cục bộ nếu có
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "comparison_artifacts", "model_comparison_results.csv")
    if os.path.exists(csv_path):
        import pandas as pd
        comp_df = pd.read_csv(csv_path)
        if "Evaluation strategy" not in comp_df.columns:
            logger.warning("Bảng so sánh cũ chưa có nhãn evaluation strategy; bỏ qua metric cũ.")
            return None
        comp_df = comp_df[comp_df["Evaluation strategy"] == REQUIRED_EVALUATION_STRATEGY]
        if comp_df.empty:
            logger.warning("Bảng so sánh chưa có kết quả '%s'.", REQUIRED_EVALUATION_STRATEGY)
            return None
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


def _current_feature_spec() -> Dict[str, Any]:
    spec_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "feature_spec.json")
    try:
        with open(spec_path, "r", encoding="utf-8") as f:
            spec = json.load(f)
        return spec if isinstance(spec, dict) else {}
    except (OSError, ValueError):
        return {}


def register_champion_model(
    experiment_name: str = "demand-forecasting-baseline",
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

    current_spec = _current_feature_spec()
    expected_origin = str(current_spec.get("data_origin", "")).strip()
    expected_datasets = sorted(set(current_spec.get("source_datasets") or []))
    if not expected_origin or current_spec.get("data_source") != "warehouse":
        logger.error("Feature Spec hiện hành thiếu nguồn warehouse đã xác định; chưa đăng ký model.")
        return {}
    if expected_origin == "public_anonymized_historical_csv" and not expected_datasets:
        logger.error("Feature Spec CSV hiện hành thiếu fingerprints source_datasets; chưa đăng ký model.")
        return {}

    tracking_uri = configure_mlflow(experiment_name=experiment_name)
    if tracking_uri.startswith("file:"):
        logger.error(
            "MLflow đang chạy ở local file store (%s), nơi không hỗ trợ Model Registry. "
            "Không tạo manifest triển khai.",
            tracking_uri,
        )
        return {}
    best_info = find_best_run(
        experiment_name,
        required_data_origin=expected_origin,
        required_source_datasets=expected_datasets,
    )

    if not best_info:
        logger.error("Không tìm thấy thông tin mô hình Champion.")
        return {}

    logger.info("🏆 Mô hình Champion được lựa chọn: %s (WAPE: %.2f%%, Run ID: %s)",
                best_info["model_name"], best_info["wape"], best_info["run_id"])

    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        if best_info["run_id"] in {"local_champion_run", "default_champion_run", None}:
            raise RuntimeError("Champion không tham chiếu tới một MLflow run có artifact thật.")
        if best_info.get("wape") is None or not math.isfinite(float(best_info["wape"])):
            raise RuntimeError("Champion run không có cv_wape hữu hạn để xác nhận chất lượng.")

        model_file = str(best_info.get("model_file") or "")
        if not model_file or os.path.basename(model_file) != model_file:
            raise RuntimeError("Run champion thiếu tên model_file hợp lệ.")

        model_uri = f"runs:/{best_info['run_id']}/model"
        artifact_dir = mlflow.artifacts.download_artifacts(
            artifact_uri=model_uri,
            tracking_uri=tracking_uri,
        )
        if not os.path.isfile(os.path.join(artifact_dir, model_file)):
            raise FileNotFoundError(f"Run artifact không có {model_file}.")
        feature_spec_path = os.path.join(artifact_dir, "feature_spec.json")
        if not os.path.isfile(feature_spec_path):
            raise FileNotFoundError("Run artifact không có feature_spec.json.")
        with open(feature_spec_path, "r", encoding="utf-8") as spec_file:
            feature_spec = json.load(spec_file)
        if feature_spec.get("data_source") != "warehouse":
            raise RuntimeError(
                "Chỉ cho đăng ký model được train từ Feature Store có nguồn warehouse thật; "
                f"artifact ghi data_source={feature_spec.get('data_source', 'unknown')}."
            )
        data_origin = str(feature_spec.get("data_origin", "")).strip().lower()
        artifact_datasets = sorted(set(feature_spec.get("source_datasets") or []))
        if data_origin != expected_origin or artifact_datasets != expected_datasets:
            raise RuntimeError(
                "Run artifact không khớp nguồn Feature Spec hiện hành; "
                "hãy train lại sau khi dựng Feature Store đúng nguồn."
            )
        blocked_origins = {"", "unknown", "unverified_warehouse", "synthetic_demo", "simulator"}
        if data_origin in blocked_origins or data_origin.startswith("local_"):
            raise RuntimeError(
                "Không đăng ký model nếu Feature Spec thiếu nguồn gốc dữ liệu đã xác định "
                f"hoặc dùng dữ liệu mô phỏng/local (data_origin={data_origin or 'unknown'})."
            )
        if data_origin == "public_anonymized_historical_csv" and not feature_spec.get("source_datasets"):
            raise RuntimeError(
                "Feature Spec từ CSV lịch sử phải ghi source_datasets để truy vết đúng bộ dữ liệu."
            )
        if not feature_spec.get("feature_columns"):
            raise RuntimeError("Feature spec không có feature_columns.")

        client = MlflowClient(tracking_uri=tracking_uri)
        try:
            client.create_registered_model(
                name=model_name,
                description="Mô hình dự báo nhu cầu hàng hóa E-Commerce cho hệ thống bán lẻ đa kênh (Shopee + TikTok Shop).",
                tags={"project": "streaming-mlops-ecommerce", "task": "demand_forecasting"},
            )
            logger.info("✓ Đã tạo Registered Model mới: %s", model_name)
        except Exception as create_error:
            try:
                client.get_registered_model(model_name)
                logger.info("Registered Model '%s' đã tồn tại trong Registry.", model_name)
            except Exception:
                raise create_error

        model_version = client.create_model_version(
            name=model_name,
            source=model_uri,
            run_id=best_info["run_id"],
            description=f"Champion model {best_info['model_name']} với CV WAPE = {best_info['wape']}%.",
            tags={
                "framework": str(best_info["model_name"]),
                "wape": str(best_info["wape"]),
                "data_origin": data_origin,
            },
        )
        registered_version = str(model_version.version)
        deadline = time.monotonic() + 60
        while True:
            model_version = client.get_model_version(model_name, registered_version)
            model_status = str(model_version.status).upper()
            if model_status.endswith("READY"):
                break
            if model_status.endswith("FAILED"):
                raise RuntimeError(
                    f"MLflow đăng ký version {registered_version} thất bại: "
                    f"{getattr(model_version, 'status_message', '')}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(f"MLflow chưa hoàn tất đăng ký version {registered_version} sau 60 giây.")
            time.sleep(1)

        client.transition_model_version_stage(
            name=model_name,
            version=registered_version,
            stage=stage,
            archive_existing_versions=True,
        )
        model_version = client.get_model_version(model_name, registered_version)
        if str(model_version.current_stage).lower() != stage.lower():
            raise RuntimeError(
                f"Version {registered_version} chưa chuyển được sang stage {stage}."
            )
        logger.info("✓ Đã đăng ký và chuyển Version %s sang trạng thái: %s", registered_version, stage)
    except Exception as e:
        logger.error("Đăng ký model thất bại (%s); giữ nguyên manifest hiện tại.", e)
        return {}

    # 2. Tạo Model Manifest JSON phục vụ trực tiếp cho FastAPI Serving (Tuần 7)
    manifest = {
        "model_registry_name": model_name,
        "version": registered_version,
        "stage": stage,
        "champion_algorithm": str(best_info["model_name"]),
        "metrics": {
            k: float(best_info[src])
            for k, src in (("cv_wape", "wape"), ("cv_mae", "mae"), ("cv_rmse", "rmse"))
            if best_info.get(src) is not None and math.isfinite(float(best_info[src]))
        },
        "run_id": best_info["run_id"],
        "model_file": best_info.get("model_file"),
        "target_variable": "daily_demand_t_plus_1",
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "input_feature_count": _feature_count(),
        "lead_time_days": 3,
        "service_level": 0.95,
        "status": "READY_FOR_SERVING",
        "data_origin": data_origin,
    }

    manifest_dir = os.path.abspath(
        manifest_dir or os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, "model_manifest.json")

    temp_path = manifest_path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, manifest_path)

    logger.info("✓ Đã xuất Model Manifest tại: %s", manifest_path)
    logger.info("═" * 60)
    logger.info("  HOÀN THÀNH QUY TRÌNH ĐĂNG KÝ MÔ HÌNH VÀO REGISTRY")
    logger.info("═" * 60)

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Đăng ký Champion Model vào MLflow Registry")
    parser.add_argument("--stage", type=str, default="Staging", choices=["Staging", "Production"])
    parser.add_argument("--experiment-name", type=str, default="demand-forecasting-baseline")
    args = parser.parse_args()

    manifest = register_champion_model(
        experiment_name=args.experiment_name,
        stage=args.stage,
    )
    if not manifest:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
