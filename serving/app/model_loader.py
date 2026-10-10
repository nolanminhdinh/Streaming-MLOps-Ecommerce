"""
model_loader.py
---------------
Quản lý vòng đời nạp mô hình (Model Lifecycle & Inference Manager):
  - Tải Champion Model: MLflow Model Registry → artifact joblib cục bộ → (không có) heuristic.
  - Nạp hợp đồng đặc trưng `feature_spec.json` (thứ tự cột, mã hóa, hồ sơ SKU) sinh ra lúc huấn luyện.
  - Dựng đặc trưng từ LỊCH SỬ NHU CẦU THẬT trong Data Warehouse (serving/app/features.py)
    và dự báo đệ quy nhiều ngày (ngày dự báo trước làm lag cho ngày sau).
  - Ràng buộc dự báo không âm (Demand >= 0).
  - Mọi kết quả đều ghi rõ nguồn: `model_source` (mô hình / heuristic) và `history_source`.

Trước bản sửa này, serving dùng bảng 20 SKU hardcode, lag/rolling là hằng số và lỗi suy luận
bị nuốt ở mức DEBUG → API trả kết quả heuristic nhưng vẫn báo "healthy".
"""

from __future__ import annotations

import glob
import json
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from serving.app.data_access import get_repository
    from serving.app.features import (
        DEFAULT_EMA_SPANS,
        DEFAULT_LAGS,
        DEFAULT_ROLLING_WINDOWS,
        build_feature_row,
        to_model_input,
    )
except ImportError:
    from app.data_access import get_repository
    from app.features import (
        DEFAULT_EMA_SPANS,
        DEFAULT_LAGS,
        DEFAULT_ROLLING_WINDOWS,
        build_feature_row,
        to_model_input,
    )

logger = logging.getLogger("mlops.serving.model_loader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
HISTORY_DAYS = int(os.getenv("SERVING_HISTORY_DAYS", "120"))
PUBLIC_HISTORY_DAYS = int(os.getenv("SERVING_PUBLIC_HISTORY_DAYS", "240"))
MAX_FORECAST_STEPS = int(os.getenv("SERVING_MAX_FORECAST_STEPS", "180"))
STATS_WINDOW_DAYS = 28

# Danh mục DỰ PHÒNG cho chế độ demo khi chưa có Data Warehouse / Feature Spec.
# Chỉ được dùng khi không có nguồn thật; response sẽ đánh dấu nguồn là "fallback_catalog".
CATALOG_METADATA: Dict[str, Dict[str, Any]] = {
    "OL-IP15PM":  {"name": "Ốp lưng iPhone 15 Pro Max",        "category": "Phụ kiện điện thoại", "base_demand": 28.5, "sigma": 4.2, "segment": "AX"},
    "ATN-CB-001": {"name": "Áo thun nam cotton Premium Basic",   "category": "Thời trang nam",     "base_demand": 16.5, "sigma": 4.0, "segment": "BY"},
    "SN-65W-GAN": {"name": "Sạc nhanh 65W GaN Type-C PD",       "category": "Phụ kiện điện tử",    "base_demand": 18.2, "sigma": 3.1, "segment": "AX"},
    "OMO-6KG":    {"name": "Bột giặt OMO Matic 6kg",            "category": "Gia dụng",            "base_demand": 22.0, "sigma": 2.8, "segment": "AX"},
    "TN-TWS-PRO": {"name": "Tai nghe Bluetooth TWS AirBuds Pro", "category": "Điện tử",            "base_demand": 15.0, "sigma": 5.4, "segment": "AY"},
    "KCN-ANESSA": {"name": "Kem chống nắng Anessa SPF50+",      "category": "Mỹ phẩm",            "base_demand": 19.5, "sigma": 6.8, "segment": "AZ"},
    "BP-GM-104":  {"name": "Bàn phím cơ Gaming RGB 104 phím",    "category": "Gaming",             "base_demand": 11.0, "sigma": 4.5, "segment": "AY"},
    "AF-6L-001":  {"name": "Nồi chiên không dầu 6L Air Fryer",  "category": "Gia dụng",            "base_demand": 8.5,  "sigma": 3.8, "segment": "BY"},
    "SRM-CERAVE": {"name": "Sữa rửa mặt CeraVe 236ml",         "category": "Mỹ phẩm",            "base_demand": 16.0, "sigma": 5.9, "segment": "AZ"},
    "GTT-JR-001": {"name": "Giày thể thao nam Jogger Runner",    "category": "Giày dép",           "base_demand": 9.2,  "sigma": 3.2, "segment": "BX"},
    "BL-15-CS":   {"name": "Balo laptop chống sốc 15.6 inch",   "category": "Túi & Balo",         "base_demand": 7.8,  "sigma": 2.5, "segment": "BX"},
    "NHH-KLA180": {"name": "Nước hoa hồng Klairs 180ml",        "category": "Mỹ phẩm",            "base_demand": 12.4, "sigma": 4.1, "segment": "BY"},
    "CG-G304":    {"name": "Chuột gaming Logitech G304",         "category": "Gaming",             "base_demand": 14.1, "sigma": 3.6, "segment": "BX"},
    "DG-CT-1M8":  {"name": "Bộ drap giường cotton 1m8",          "category": "Gia dụng",            "base_demand": 6.0,  "sigma": 2.0, "segment": "BY"},
    "DEN-LED-10": {"name": "Đèn LED dây trang trí 10m",          "category": "Decor",              "base_demand": 13.5, "sigma": 6.2, "segment": "BZ"},
    "SR-VC-TO30": {"name": "Serum Vitamin C The Ordinary 30ml",  "category": "Mỹ phẩm",            "base_demand": 14.8, "sigma": 5.5, "segment": "AY"},
    "KT-KF94-10": {"name": "Combo 10 khẩu trang 3D KF94",       "category": "Sức khoẻ",           "base_demand": 35.0, "sigma": 7.5, "segment": "AX"},
    "QM-USB-01":  {"name": "Quạt mini cầm tay sạc USB",         "category": "Gia dụng",            "base_demand": 8.0,  "sigma": 3.9, "segment": "BY"},
    "TX-PU-001":  {"name": "Túi xách nữ da PU thời trang",      "category": "Thời trang nữ",      "base_demand": 5.5,  "sigma": 2.8, "segment": "CY"},
    "CL-SS-S24":  {"name": "Miếng dán cường lực Samsung S24",   "category": "Phụ kiện điện thoại","base_demand": 21.0, "sigma": 4.8, "segment": "AX"},
}
DEFAULT_PRODUCT = {"name": "Sản phẩm {sku}", "category": "Khác", "base_demand": 10.0, "sigma": 3.0, "segment": "BX"}

MEGA_SALE_DAYS = {(m, m) for m in range(1, 13)}

# Nguồn mô hình
SOURCE_MLFLOW = "mlflow_registry"
SOURCE_JOBLIB = "local_joblib"
SOURCE_HEURISTIC_HISTORY = "heuristic_history"   # không có mô hình, nhưng baseline lấy từ lịch sử thật
SOURCE_HEURISTIC_CATALOG = "heuristic_catalog"   # không có mô hình lẫn dữ liệu → bảng demo


def _heuristic_factors(d: date) -> Tuple[float, float, float]:
    """Hệ số thứ trong tuần / Mega-sale / ngày lương cho chế độ heuristic dự phòng."""
    dow_factor = {0: 0.95, 1: 0.98, 2: 1.02, 3: 1.00, 4: 1.10, 5: 1.35, 6: 1.25}[d.weekday()]
    mega_factor = 3.5 if (d.month, d.day) in MEGA_SALE_DAYS else 1.0
    payday_factor = 1.15 if 25 <= d.day <= 28 else 1.0
    return dow_factor, mega_factor, payday_factor


def _mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    m = sum(values) / len(values)
    if len(values) < 2:
        return m, 0.0
    return m, math.sqrt(sum((v - m) ** 2 for v in values) / (len(values) - 1))


class ModelManager:
    """Singleton quản lý mô hình Machine Learning phục vụ dự báo nhu cầu."""

    _instance: Optional["ModelManager"] = None

    def __init__(self, manifest_path: Optional[str] = None):
        self.manifest_path = os.path.abspath(manifest_path or os.getenv(
            "MODEL_MANIFEST_PATH", os.path.join(DATA_DIR, "model_manifest.json")
        ))
        self.model = None
        self.model_source: str = SOURCE_HEURISTIC_CATALOG
        self.model_artifact: Optional[str] = None
        self.feature_spec: Dict[str, Any] = {}
        self.manifest: Dict[str, Any] = {}
        self.is_loaded: bool = False  # đã chạy xong quy trình khởi tạo (kể cả khi rơi về heuristic)
        self.load_timestamp: Optional[datetime] = None

    @classmethod
    def get_instance(cls, manifest_path: Optional[str] = None) -> "ModelManager":
        if cls._instance is None:
            cls._instance = cls(manifest_path=manifest_path)
        return cls._instance

    @property
    def has_model(self) -> bool:
        """True khi đang phục vụ bằng mô hình ML thật (không phải heuristic)."""
        return self.model is not None

    # ─────────────────────────────────────────────────────────
    # NẠP MÔ HÌNH
    # ─────────────────────────────────────────────────────────

    def load_model(self) -> bool:
        """Nạp model vào trạng thái tạm rồi đổi nguyên khối khi artifact hợp lệ.

        Khi reload thất bại, model đang phục vụ tiếp tục được giữ nguyên. Điều này
        tránh request đồng thời nhìn thấy trạng thái rỗng trong lúc tải artifact.
        """
        candidate = ModelManager(manifest_path=self.manifest_path)
        candidate._load_model_candidate()

        if not candidate.has_model and self.has_model:
            logger.error(
                "Reload model thất bại; giữ nguyên model %s v%s đang phục vụ.",
                self.model_source,
                self.manifest.get("version", "unknown"),
            )
            return False

        self.__dict__ = candidate.__dict__.copy()
        return self.has_model

    def _load_model_candidate(self) -> None:
        logger.info("Đang nạp Champion Model từ manifest: %s", self.manifest_path)
        if os.path.exists(self.manifest_path):
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    self.manifest = json.load(f)
                logger.info("✓ Nạp manifest: %s v%s [%s]", self.manifest.get("model_registry_name"),
                            self.manifest.get("version"), self.manifest.get("stage"))
            except Exception as e:
                logger.warning("Lỗi đọc manifest (%s), dùng cấu hình mặc định.", e)
                self.manifest = self._default_manifest()
        else:
            logger.info("Chưa có manifest, dùng cấu hình mặc định.")
            self.manifest = self._default_manifest()

        self.model, self.model_source, self.model_artifact = None, SOURCE_HEURISTIC_CATALOG, None
        spec_dir = self._try_load_mlflow()
        if not spec_dir and not os.getenv("MLFLOW_TRACKING_URI"):
            # Chỉ dùng artifact cục bộ khi người vận hành chủ động chạy không có
            # Registry. Khi đã cấu hình Registry, không tráo sang một file có thể
            # thuộc version khác nếu server/artifact của version được ghim bị lỗi.
            spec_dir = self._try_load_local_joblib()
        self._load_feature_spec(spec_dir)

        if self.model is None:
            logger.warning(
                "⚠ KHÔNG nạp được mô hình ML nào — API chạy ở chế độ HEURISTIC DỰ PHÒNG "
                "(/health sẽ báo 'degraded'). Hãy chạy train_baseline + register_model."
            )
        self.is_loaded = True
        self.load_timestamp = datetime.now(timezone.utc)

    def _default_manifest(self) -> Dict[str, Any]:
        return {
            "model_registry_name": "ECommerceDemandForecastModel",
            "version": "0",
            "stage": "Staging",
            "champion_algorithm": "none",
            "metrics": {},
            "lead_time_days": 3,
            "service_level": 0.95,
            "input_feature_count": 0,
            "status": "NO_REGISTERED_MODEL",
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }

    def _load_joblib(self, path: str) -> bool:
        try:
            import joblib
            model = joblib.load(path)
        except Exception as e:
            logger.warning("Không nạp được artifact %s: %s", path, e)
            return False
        if not hasattr(model, "predict"):
            logger.warning("Artifact %s không có phương thức predict — bỏ qua.", path)
            return False
        self.model, self.model_artifact = model, path
        return True

    def _try_load_mlflow(self) -> Optional[str]:
        """Tải artifact joblib của version đang ở `stage` từ MLflow Registry. Trả về thư mục artifact."""
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
        if not tracking_uri:
            return None
        # MLflow mặc định retry HTTP nhiều lần → khởi động API bị treo hàng phút khi server tắt
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
        name = self.manifest.get("model_registry_name", "ECommerceDemandForecastModel")
        stage = self.manifest.get("stage", "Staging")
        try:
            import mlflow
            from mlflow.tracking import MlflowClient

            client = MlflowClient(tracking_uri=tracking_uri)
            version = str(self.manifest.get("version", "0"))
            if version.isdigit() and int(version) > 0:
                # Manifest là phiên bản triển khai đã được xác nhận; không tự ý
                # chuyển sang version mới nhất chỉ vì Registry có version khác.
                mv = client.get_model_version(name, version)
            else:
                versions = client.get_latest_versions(name, stages=[stage])
                if not versions:
                    logger.info("MLflow Registry chưa có version nào của %s ở stage %s.", name, stage)
                    return None
                mv = versions[0]
            local_dir = mlflow.artifacts.download_artifacts(artifact_uri=mv.source, tracking_uri=tracking_uri)
        except Exception as e:
            logger.warning("Không tải được mô hình từ MLflow Registry (%s): %s", tracking_uri, e)
            return None

        preferred = self.manifest.get("model_file")
        candidates = ([os.path.join(local_dir, preferred)] if preferred else []) + sorted(
            glob.glob(os.path.join(local_dir, "**", "*.joblib"), recursive=True)
        )
        for path in candidates:
            if os.path.exists(path) and self._load_joblib(path):
                self.model_source = SOURCE_MLFLOW
                self.manifest["version"] = str(mv.version)
                logger.info("✓ Nạp mô hình từ MLflow Registry: %s v%s (%s)", name, mv.version, path)
                return os.path.dirname(path)
        logger.warning("Version %s của %s không chứa artifact *.joblib hợp lệ.", mv.version, name)
        return None

    def _try_load_local_joblib(self) -> Optional[str]:
        preferred = self.manifest.get("model_file")
        candidates = []
        if preferred:
            candidates.append(os.path.join(DATA_DIR, "mlflow_artifacts", preferred))
        candidates += [
            os.path.join(DATA_DIR, "champion_model.joblib"),
            os.path.join(DATA_DIR, "mlflow_artifacts", "lightgbm_model.joblib"),
        ]
        for path in candidates:
            if os.path.exists(path) and self._load_joblib(path):
                self.model_source = SOURCE_JOBLIB
                logger.info("✓ Nạp mô hình cục bộ: %s", path)
                return None
        return None

    def _load_feature_spec(self, artifact_dir: Optional[str]) -> None:
        paths = []
        if artifact_dir:
            paths.append(os.path.join(artifact_dir, "feature_spec.json"))
        paths.append(os.getenv("FEATURE_SPEC_PATH", os.path.join(DATA_DIR, "feature_spec.json")))
        for path in paths:
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        self.feature_spec = json.load(f)
                    logger.info("✓ Nạp Feature Spec: %s (%d đặc trưng)", path,
                                len(self.feature_spec.get("feature_columns", [])))
                    return
                except Exception as e:
                    logger.warning("Lỗi đọc Feature Spec %s: %s", path, e)
        self.feature_spec = {}
        if self.model is not None:
            names = getattr(self.model, "feature_names_in_", None)
            if names is None:
                logger.warning("Có mô hình nhưng thiếu feature_spec.json và feature_names_in_ → không thể "
                               "dựng đúng đầu vào, chuyển sang heuristic.")
                self.model, self.model_source = None, SOURCE_HEURISTIC_CATALOG
            else:
                logger.warning("Thiếu feature_spec.json — dùng feature_names_in_ của mô hình, "
                               "mã hóa phân khúc SKU sẽ mặc định -1.")
                self.feature_spec = {"feature_columns": list(names)}

    # ─────────────────────────────────────────────────────────
    # DANH MỤC SẢN PHẨM
    # ─────────────────────────────────────────────────────────

    def get_catalog(self) -> Tuple[Dict[str, Dict[str, Any]], str]:
        """Danh mục SKU: Data Warehouse → Feature Spec → bảng demo. Trả về (catalog, source)."""
        data_origin = self.feature_spec.get("data_origin")
        catalog = get_repository().get_catalog(data_origin=data_origin)
        if catalog:
            return catalog, "warehouse"
        profiles = self.feature_spec.get("sku_profiles") or {}
        if profiles:
            return {
                sku: {"name": p.get("product_name") or sku, "category": p.get("category") or "Khác"}
                for sku, p in profiles.items()
            }, "feature_spec"
        return {sku: {"name": m["name"], "category": m["category"]} for sku, m in CATALOG_METADATA.items()}, \
            "fallback_catalog"

    def get_product_info(self, sku: str) -> Dict[str, Any]:
        info = dict(CATALOG_METADATA.get(sku) or {**DEFAULT_PRODUCT, "name": f"Sản phẩm {sku}"})
        profile = (self.feature_spec.get("sku_profiles") or {}).get(sku) or {}
        catalog = get_repository().get_catalog(data_origin=self.feature_spec.get("data_origin")) or {}
        if sku in catalog:
            info.update(catalog[sku])
        elif profile:
            info["name"] = profile.get("product_name") or info["name"]
            info["category"] = profile.get("category") or info["category"]
        if profile.get("matrix_class"):
            info["segment"] = profile["matrix_class"]
        return info

    # ─────────────────────────────────────────────────────────
    # DỰ BÁO
    # ─────────────────────────────────────────────────────────

    def _load_history(self, sku: str, from_date: date) -> Tuple[Optional[List[Dict[str, Any]]], Optional[date]]:
        """Lịch sử thật kết thúc tại min(from_date - 1, ngày cuối có dữ liệu)."""
        repo = get_repository()
        if not repo.available():
            return None, None
        data_origin = self.feature_spec.get("data_origin")
        last_known = repo.get_last_order_date(data_origin=data_origin)
        if last_known is None:
            return None, None
        hist_end = min(from_date - timedelta(days=1), last_known)
        history_days = (
            max(HISTORY_DAYS, PUBLIC_HISTORY_DAYS)
            if data_origin == "public_anonymized_historical_csv"
            else HISTORY_DAYS
        )
        history = repo.get_daily_history(
            sku, hist_end, days=history_days, data_origin=data_origin
        )
        return (history, hist_end) if history else (None, None)

    def _model_predict_one(self, history: List[Dict[str, Any]], row_date: date, sku: str) -> float:
        import pandas as pd

        spec = self.feature_spec
        cols = spec["feature_columns"]
        row = build_feature_row(
            history, row_date,
            profile=(spec.get("sku_profiles") or {}).get(sku),
            category_mappings=spec.get("category_mappings"),
            lags=spec.get("lags", DEFAULT_LAGS),
            rolling_windows=spec.get("rolling_windows", DEFAULT_ROLLING_WINDOWS),
            ema_spans=spec.get("ema_spans", DEFAULT_EMA_SPANS),
        )
        x = pd.DataFrame([to_model_input(row, cols)], columns=cols)
        raw = self.model.predict(x)
        return max(0.0, float(raw[0] if hasattr(raw, "__len__") else raw))

    def forecast(
        self,
        sku: str,
        from_date: date,
        to_date: date,
        confidence_interval: bool = False,
    ) -> Dict[str, Any]:
        """
        Dự báo chuỗi nhu cầu [from_date, to_date].

        - Có mô hình + lịch sử thật: dự báo đệ quy từ ngày cuối có dữ liệu; mỗi ngày dự báo
          được nối vào chuỗi để làm lag/rolling cho ngày kế tiếp.
        - Chỉ có lịch sử: heuristic với baseline = trung bình 28 ngày gần nhất.
        - Không có gì: heuristic với bảng demo (đánh dấu rõ nguồn).

        Returns:
            {"items": [...], "model_source": str, "history_source": str}
        """
        history, hist_end = self._load_history(sku, from_date)
        recent = [h["daily_demand"] for h in history[-STATS_WINDOW_DAYS:]] if history else []
        hist_mean, hist_std = _mean_std(recent)

        preds: Dict[date, float] = {}
        model_source = self.model_source
        if self.has_model and history and self.feature_spec.get("feature_columns"):
            steps = (to_date - hist_end).days
            if steps > MAX_FORECAST_STEPS:
                raise ValueError(
                    f"Khoảng dự báo cách dữ liệu cuối ({hist_end}) {steps} ngày, vượt giới hạn "
                    f"{MAX_FORECAST_STEPS} bước đệ quy."
                )
            series = [dict(h) for h in history]
            tot_orders = sum(h["order_count"] for h in series[-STATS_WINDOW_DAYS:])
            qty_per_order = (sum(recent) / tot_orders) if tot_orders else 1.0
            d = hist_end
            try:
                while d < to_date:
                    y_hat = self._model_predict_one(series, d, sku)
                    d = d + timedelta(days=1)
                    price = series[-1]["avg_unit_price"] or 0.0
                    series.append({
                        "date": d,
                        "daily_demand": y_hat,
                        "daily_revenue": y_hat * price,
                        "avg_unit_price": price,
                        "total_discount": 0.0,
                        "order_count": y_hat / qty_per_order if qty_per_order else 0.0,
                    })
                    if d >= from_date:
                        preds[d] = y_hat
            except Exception as e:
                # Không nuốt lỗi âm thầm: log WARNING và đánh dấu nguồn heuristic trong response
                logger.warning("Suy luận mô hình lỗi cho SKU %s (%s) → heuristic dự phòng.", sku, e)
                preds = {}

        if not preds:
            base = hist_mean if history else self.get_product_info(sku)["base_demand"]
            model_source = SOURCE_HEURISTIC_HISTORY if history else SOURCE_HEURISTIC_CATALOG
            d = from_date
            while d <= to_date:
                dow, mega, payday = _heuristic_factors(d)
                preds[d] = max(0.0, base * dow * mega * payday)
                d += timedelta(days=1)

        sigma = hist_std if history else self.get_product_info(sku)["sigma"]
        items = []
        for d in sorted(preds):
            qty = round(preds[d], 1)
            item: Dict[str, Any] = {"date": d.strftime("%Y-%m-%d"), "predicted_quantity": qty}
            if confidence_interval:
                # Khoảng ±1.96σ với σ = độ lệch chuẩn nhu cầu 28 ngày gần nhất (xấp xỉ, không phải
                # khoảng dự báo hiệu chuẩn của mô hình); heuristic nới rộng theo hệ số Mega-sale.
                scale = math.sqrt(_heuristic_factors(d)[1]) if model_source.startswith("heuristic") else 1.0
                margin = 1.96 * sigma * scale
                item["lower_bound"] = max(0.0, round(qty - margin, 1))
                item["upper_bound"] = round(qty + margin, 1)
            items.append(item)

        return {
            "items": items,
            "model_source": model_source,
            "history_source": "warehouse" if history else "none",
            "history_end": str(hist_end) if hist_end else None,
        }

    def predict_range(
        self,
        sku: str,
        from_date: date,
        to_date: date,
        confidence_interval: bool = False,
    ) -> List[Dict[str, Any]]:
        """Dự báo chuỗi nhu cầu từ from_date đến to_date (giữ API cũ)."""
        return self.forecast(sku, from_date, to_date, confidence_interval)["items"]

    def predict_daily(
        self,
        sku: str,
        target_date: date,
        confidence_interval: bool = False,
    ) -> Tuple[float, Optional[float], Optional[float]]:
        """Dự báo 1 ngày (giữ API cũ): trả về (y_hat, lower, upper)."""
        item = self.predict_range(sku, target_date, target_date, confidence_interval)[0]
        return item["predicted_quantity"], item.get("lower_bound"), item.get("upper_bound")

    def get_metadata(self) -> Dict[str, Any]:
        if not self.is_loaded:
            self.load_model()
        meta = self.manifest.copy()
        meta["model_source"] = self.model_source
        meta["model_ready"] = self.has_model
        meta["training_data_origin"] = self.feature_spec.get("data_origin", "unknown")
        if self.feature_spec.get("feature_columns"):
            meta["input_feature_count"] = len(self.feature_spec["feature_columns"])
        return meta


def get_model_manager() -> ModelManager:
    manager = ModelManager.get_instance()
    if not manager.is_loaded:
        manager.load_model()
    return manager
