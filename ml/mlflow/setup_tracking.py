"""
setup_tracking.py
-----------------
Quản lý cấu hình kết nối MLflow Tracking Server:
  - Kết nối tới MLflow Tracking Server (http://localhost:5000)
  - Cấu hình biến môi trường kết nối MinIO S3 lưu trữ artifact.
  - Tự động tạo và kích hoạt Experiment: `demand-forecasting-baseline`.
  - Hỗ trợ chế độ Local Fallback (file:./mlruns) nếu Docker container MLflow chưa bật.

Tuần 5: Thiết lập hạ tầng MLOps Experiment Tracking.
"""

import logging
import os
import sys
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger("mlops.tracking")


def configure_mlflow(
    experiment_name: str = "demand-forecasting-baseline",
    tracking_uri: Optional[str] = None,
) -> str:
    """
    Cấu hình MLflow và trả về URI theo dõi đang hoạt động.
    """
    try:
        import mlflow
    except ImportError:
        logger.warning("MLflow chưa được cài đặt trong môi trường hiện tại (chạy ngoài .venv). Chuyển sang chế độ Local/Mock.")
        return "file:./mlruns"

    # 1. Đọc cấu hình từ biến môi trường
    uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    s3_endpoint = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
    aws_key = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
    aws_secret = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret

    # 2. Kiểm tra kết nối tới MLflow Tracking Server
    use_local_fallback = False
    try:
        import urllib.request
        req = urllib.request.Request(f"{uri.rstrip('/')}/health", method="GET")
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                logger.info("Kết nối thành công tới MLflow Server tại: %s", uri)
    except Exception:
        # Nếu chưa mở được port 5000, kiểm tra port root
        try:
            import urllib.request
            req = urllib.request.Request(uri, method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                pass
            logger.info("Kết nối thành công tới MLflow Server tại: %s", uri)
        except Exception as e:
            logger.warning(
                "Không thể kết nối MLflow Server tại %s (%s). Tự động chuyển sang Local Fallback (file:./mlruns).",
                uri, e,
            )
            use_local_fallback = True

    if use_local_fallback:
        local_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "mlruns"))
        os.makedirs(local_dir, exist_ok=True)
        uri = f"file:///{local_dir.replace(os.sep, '/')}"

    mlflow.set_tracking_uri(uri)

    # 3. Tạo hoặc kích hoạt Experiment
    try:
        exp = mlflow.get_experiment_by_name(experiment_name)
        if exp is None:
            exp_id = mlflow.create_experiment(
                name=experiment_name,
                tags={
                    "project": "streaming-mlops-ecommerce",
                    "task": "demand-forecasting",
                    "framework": "lightgbm-xgboost",
                },
            )
            logger.info("Đã tạo mới Experiment: %s (ID: %s)", experiment_name, exp_id)
        else:
            exp_id = exp.experiment_id
            logger.info("Sử dụng Experiment có sẵn: %s (ID: %s)", experiment_name, exp_id)

        mlflow.set_experiment(experiment_name)
    except Exception as e:
        logger.error("Lỗi khi cấu hình Experiment: %s", e)

    return uri


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    active_uri = configure_mlflow()
    print(f"MLflow Tracking URI hiện tại: {active_uri}")
