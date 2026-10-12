"""Generate Python ABC/XYZ reference results for the isolated PostgreSQL checks."""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ml.features.abc_xyz import ABCXYZClassifier

cases = [
    [("A", "2026-07-01", 10, 1000), ("B", "2026-07-03", 1, 1)],
    [("A", "2026-07-01", 10, 0), ("B", "2026-07-03", 1, 0)],
    [("A", "2026-07-01", 1, 5), ("B", "2026-07-03", 1, 5), ("C", "2026-07-01", 1, 5)],
    [("A", "2026-07-01", 3, 10), ("B", "2026-07-01", 2, 4)],
    [("A", "2026-07-01", 1, 10), ("A", "2026-07-02", 2, 10), ("A", "2026-07-03", 3, 10)],
]
result = []
for records in cases:
    orders = [dict(zip(["sku", "create_time", "quantity", "buyer_total_amount"], row)) for row in records]
    expected = ABCXYZClassifier().fit_transform(pd.DataFrame(orders))
    result.append({"orders": orders, "expected": expected[[
        "sku", "abc_class", "xyz_class", "avg_daily_demand", "std_daily_demand", "safety_stock", "reorder_point",
    ]].to_dict("records")})
path = Path(sys.argv[1])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(result), encoding="utf-8")
