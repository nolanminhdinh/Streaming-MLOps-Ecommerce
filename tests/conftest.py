"""
Cấu hình chung cho pytest.

Unit test không được phụ thuộc PostgreSQL / MLflow đang chạy: tắt truy cập Data Warehouse
của tầng serving (các test cần dữ liệu kho dùng repository giả — xem test_serving_realdata.py).
"""

import os
import sys

os.environ.setdefault("SERVING_USE_WAREHOUSE", "false")
os.environ.pop("MLFLOW_TRACKING_URI", None)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Khắc phục lỗi tương thích Python 3.12 + LightGBM trên Windows:
# Tránh crash ctypes stack alignment / specializing adaptive interpreter
try:
    import lightgbm as lgb
    _orig_safe_call = lgb.basic._safe_call

    def _safe_call_wrapper(ret):
        return _orig_safe_call(ret)

    lgb.basic._safe_call = _safe_call_wrapper
except Exception:
    pass
