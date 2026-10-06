"""
trigger_retraining.py
---------------------
Quy trình Tái huấn luyện Vòng lặp Khép kín (Closed-Loop Retraining Trigger):
  1. Đọc kết quả phân tích Data Drift từ drift_summary.json (drift_detector.py).
  2. Đánh giá điều kiện kích hoạt:
     - Tỷ lệ features bị drift >= ngưỡng (dataset drift)
     - Hoặc có Target/Concept Drift trên biến daily_demand.
     Summary sinh từ dữ liệu giả lập (data_source = "synthetic_demo") KHÔNG kích hoạt
     retrain tự động, trừ khi gọi force_retrain.
  3. Dựng lại Feature Store từ Data Warehouse và huấn luyện lại (train_baseline).
  4. Đăng ký mô hình mới qua register_model (MLflow Registry + manifest).
     Huấn luyện / đăng ký thất bại → KHÔNG tăng version, KHÔNG ghi metrics giả,
     audit log ghi trạng thái FAILED.
  5. Gọi POST /model/reload của FastAPI để serving nạp mô hình mới (nếu cấu hình URL).
  6. Ghi nhật ký kiểm toán: data/monitoring_reports/retraining_log.json.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("mlops.monitoring.retraining")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RETRAIN_EXPERIMENT = "demand-forecasting-retraining"


def default_train_fn() -> Dict[str, Any]:
    """Dựng lại Feature Store từ dữ liệu mới nhất rồi huấn luyện lại; trả về metrics của mô hình tốt nhất."""
    sys.path.insert(0, PROJECT_ROOT)
    from ml.features.feature_pipeline import build_feature_store, load_orders_data
    from ml.training.train_baseline import run_training_pipeline

    build_feature_store(load_orders_data())
    summary_df = run_training_pipeline(
        experiment_name=RETRAIN_EXPERIMENT,
        models_to_run=["lightgbm", "moving_average"],
        n_splits=2,
    )
    if summary_df is None or summary_df.empty:
        raise RuntimeError("Pipeline huấn luyện không trả về kết quả")
    best = summary_df.iloc[0]
    return {
        "model_name": str(best["Mô hình"]),
        "cv_wape": float(best["WAPE (%)"]),
        "cv_mae": float(best["MAE"]),
        "cv_rmse": float(best["RMSE"]),
    }


def default_register_fn(manifest_dir: str) -> Dict[str, Any]:
    sys.path.insert(0, PROJECT_ROOT)
    from ml.training.register_model import register_champion_model

    return register_champion_model(experiment_name=RETRAIN_EXPERIMENT, stage="Staging", manifest_dir=manifest_dir)


def notify_serving_reload(url: Optional[str] = None) -> bool:
    """Yêu cầu FastAPI nạp lại mô hình (POST /model/reload). Lỗi chỉ được log, không làm hỏng retrain."""
    url = url or os.getenv("SERVING_RELOAD_URL")
    if not url:
        logger.info("Chưa cấu hình SERVING_RELOAD_URL — hãy restart / gọi /model/reload thủ công.")
        return False
    try:
        import urllib.request

        req = urllib.request.Request(url, method="POST", data=b"")
        token = os.getenv("MODEL_RELOAD_TOKEN")
        if token:
            req.add_header("X-Reload-Token", token)
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = 200 <= resp.status < 300
        logger.info("✓ Đã yêu cầu serving nạp lại mô hình: %s (HTTP %s)", url, resp.status)
        return ok
    except Exception as e:
        logger.warning("Không gọi được %s để reload mô hình: %s", url, e)
        return False


class ClosedLoopRetrainer:
    """Bộ điều phối tự động kích hoạt huấn luyện lại khi có sự cố trôi dạt dữ liệu."""

    def __init__(
        self,
        summary_path: Optional[str] = None,
        manifest_path: Optional[str] = None,
        retraining_log_path: Optional[str] = None,
        train_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        register_fn: Optional[Callable[[str], Dict[str, Any]]] = None,
        reload_fn: Optional[Callable[[], bool]] = None,
    ):
        base_data = os.path.join(PROJECT_ROOT, "data")
        self.summary_path = os.path.abspath(
            summary_path or os.path.join(base_data, "monitoring_reports", "drift_summary.json"))
        self.manifest_path = os.path.abspath(manifest_path or os.path.join(base_data, "model_manifest.json"))
        self.retraining_log_path = os.path.abspath(
            retraining_log_path or os.path.join(base_data, "monitoring_reports", "retraining_log.json"))
        self.train_fn = train_fn or default_train_fn
        self.register_fn = register_fn or default_register_fn
        self.reload_fn = reload_fn or notify_serving_reload

    def _read_json(self, path: str) -> Dict[str, Any]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Không đọc được %s: %s", path, e)
        return {}

    def _write_audit(self, entry: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.retraining_log_path), exist_ok=True)
        with open(self.retraining_log_path, "w", encoding="utf-8") as f:
            json.dump(entry, f, indent=2, ensure_ascii=False)
        logger.info("✓ Đã ghi Retraining Audit Log tại: %s", self.retraining_log_path)

    def evaluate_and_trigger(self, force_retrain: bool = False) -> Dict[str, Any]:
        """Kiểm tra điều kiện drift và tiến hành tái huấn luyện nếu cần thiết."""
        logger.info("═" * 65)
        logger.info("  KIỂM TRA ĐIỀU KIỆN CLOSED-LOOP RETRAINING PIPELINE")
        logger.info("═" * 65)

        if not os.path.exists(self.summary_path) and not force_retrain:
            logger.warning("Chưa tìm thấy tệp %s. Bỏ qua kiểm tra.", self.summary_path)
            return {"status": "SKIPPED", "reason": "No drift summary found"}

        drift_info = self._read_json(self.summary_path)
        drift_detected = bool(drift_info.get("drift_detected", False))
        drift_share = drift_info.get("drift_share", 0.0)
        target_drift = drift_info.get("target_drift_detected", False)
        data_source = drift_info.get("data_source", "unknown")

        if drift_detected and data_source == "synthetic_demo" and not force_retrain:
            logger.warning("Drift summary đến từ dữ liệu GIẢ LẬP → không tự động retrain (dùng force_retrain nếu muốn demo).")
            return {"status": "SKIPPED", "reason": "Drift summary is synthetic demo data"}

        if not drift_detected and not force_retrain:
            logger.info("✓ Không phát hiện trôi dạt dữ liệu ý nghĩa. Không cần tái huấn luyện.")
            return {
                "status": "SKIPPED",
                "reason": "Model and Data distribution are healthy",
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            }

        trigger = "manual_force" if (force_retrain and not drift_detected) else "data_drift"
        logger.warning("🚨 KÍCH HOẠT TÁI HUẤN LUYỆN (%s) | drift_share=%.1f%% | target_drift=%s | nguồn=%s",
                       trigger, float(drift_share) * 100, target_drift, data_source)

        old_manifest = self._read_json(self.manifest_path)
        current_version = str(old_manifest.get("version", "0"))
        started_at = datetime.now(timezone.utc).isoformat()
        audit_entry: Dict[str, Any] = {
            "event": "CLOSED_LOOP_RETRAINING_TRIGGERED",
            "trigger": trigger,
            "timestamp": started_at,
            "old_version": current_version,
            "drift_share": drift_share,
            "drift_data_source": data_source,
            "drifted_features": drift_info.get("drifted_features", []),
        }

        # 1. Huấn luyện lại — lỗi thì dừng, giữ nguyên mô hình đang phục vụ
        try:
            train_metrics = self.train_fn()
            logger.info("✓ Huấn luyện lại thành công: %s", train_metrics)
        except Exception as e:
            logger.error("✗ Huấn luyện lại THẤT BẠI: %s — giữ nguyên mô hình v%s.", e, current_version)
            audit_entry.update({"status": "FAILED", "stage": "training", "error": str(e)})
            self._write_audit(audit_entry)
            return {"status": "FAILED", "stage": "training", "error": str(e), "audit_log": audit_entry}

        # 2. Đăng ký mô hình mới (MLflow Registry + manifest cho serving)
        manifest_dir = os.path.dirname(self.manifest_path)
        try:
            new_manifest = self.register_fn(manifest_dir) or {}
        except Exception as e:
            new_manifest = {}
            logger.error("✗ Đăng ký mô hình thất bại: %s", e)
        if not new_manifest:
            audit_entry.update({"status": "FAILED", "stage": "registration", "train_metrics": train_metrics})
            self._write_audit(audit_entry)
            return {"status": "FAILED", "stage": "registration", "audit_log": audit_entry}

        # Registry offline → register_model trả version "1"; vẫn phải tăng so với bản đang chạy
        new_version = str(new_manifest.get("version", "1"))
        if new_manifest.get("run_id") in (None, "local_champion_run") and current_version.isdigit():
            new_version = str(int(current_version) + 1)
        new_manifest["version"] = new_version
        new_manifest["retraining_metadata"] = {
            "triggered_by": trigger,
            "drift_share_trigger": drift_share,
            "target_drift_trigger": target_drift,
            "drift_data_source": data_source,
            "previous_version": current_version,
            "train_metrics": train_metrics,
        }
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(new_manifest, f, indent=2, ensure_ascii=False)
        logger.info("✓ Đã cập nhật Model Manifest: version %s → %s", current_version, new_version)

        # 3. Báo serving nạp lại mô hình
        reloaded = self.reload_fn()

        audit_entry.update({
            "status": "SUCCESS",
            "new_version": new_version,
            "new_metrics": new_manifest.get("metrics", {}),
            "serving_reloaded": reloaded,
        })
        self._write_audit(audit_entry)

        logger.info("═" * 65)
        logger.info("  HOÀN TẤT TÁI HUẤN LUYỆN KHÉP KÍN -> MODEL V%s SẴN SÀNG", new_version)
        logger.info("═" * 65)
        return {"status": "RETRAINED", "new_version": new_version, "audit_log": audit_entry}


def run_trigger(force_retrain: bool = False) -> Dict[str, Any]:
    """Hàm chạy nhanh điều phối tái huấn luyện."""
    return ClosedLoopRetrainer().evaluate_and_trigger(force_retrain=force_retrain)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Closed-loop retraining theo kết quả drift")
    parser.add_argument("--force", action="store_true", help="Bắt buộc huấn luyện lại (demo)")
    args = parser.parse_args()
    run_trigger(force_retrain=args.force)
