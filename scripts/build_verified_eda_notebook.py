"""Execute and record a small read-only EDA notebook in-process.

This avoids requiring a Jupyter kernel/socket. Every saved output comes from
executed code; metadata states the execution method. No source data is written.
"""
from pathlib import Path
import base64
from contextlib import redirect_stdout
import io
import json

ROOT = Path(__file__).resolve().parents[1]
cells = []


def markdown(source):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": source})


namespace = {"__name__": "__eda_notebook__"}
count = 0


def code(source):
    global count
    count += 1
    outputs = []
    def display_png(path):
        outputs.append({"output_type": "display_data", "metadata": {},
                        "data": {"image/png": base64.b64encode(Path(path).read_bytes()).decode("ascii")}})
    namespace["display_png"] = display_png
    stream = io.StringIO()
    with redirect_stdout(stream):
        exec(compile(source, f"verified_eda_cell_{count}", "exec"), namespace)
    if stream.getvalue(): outputs.insert(0, {"output_type": "stream", "name": "stdout", "text": stream.getvalue()})
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": count,
                  "source": source, "outputs": outputs})


markdown("# EDA được kiểm chứng trên snapshot lịch sử\n\nNgày 07/11/2026. Đọc dữ liệu đã tổng hợp và hồ sơ đánh giá offline; không nạp lại CSV thô, không xác nhận coverage, không training/register/reload. Các output được thực thi bằng Python trong cùng tiến trình, không qua kernel Jupyter; có thể chạy lại từ đầu trong notebook. Không dùng dữ liệu mô phỏng để suy luận thị phần thực tế.")
code("""from pathlib import Path
import hashlib, json, sys
import pandas as pd
root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / 'data/daily_history.parquet').exists())
sys.path.insert(0, str(root))
evidence = root / 'docs/07-bao-cao-do-an/evidence/remediation-2026-11-07'
result = json.loads((evidence / 'baseline-evaluation.json').read_text(encoding='utf-8'))
path = root / 'data/daily_history.parquet'
saved_hash = next(v for k, v in result['source_sha256'].items() if k.replace(chr(92), '/') == 'data/daily_history.parquet')
assert hashlib.sha256(path.read_bytes()).hexdigest() == saved_hash
daily = pd.read_parquet(path)
print('rows:', len(daily), 'SKUs:', daily.sku.nunique(), 'days:', daily.date.nunique())
print('dates:', daily.date.min(), daily.date.max())
print('source fingerprint verified; snapshot read only')
""")
code("""print(daily.daily_demand.describe().to_string())
print('median:', daily.daily_demand.median())
print('zero_fraction:', daily.daily_demand.eq(0).mean())
print('duplicate_sku_date:', daily.duplicated(['sku', 'date']).sum())
print('missing_by_column:')
print(daily.isna().sum().to_string())
""")
markdown("**Diễn giải:** mean và median khác nhau lớn, phần lớn ô bằng 0. Snapshot được tạo sau zero-fill, nên tỷ lệ 0 chưa phải tỷ lệ ngày không bán đã được nguồn chứng nhận. Cột category thiếu toàn bộ; giá/doanh thu của nguồn lịch sử không mang đủ thông tin để diễn giải tài chính. Không có khóa SKU/ngày trùng trong snapshot không chứng minh CSV gốc chưa từng có dòng trùng.")
code("""display_png(evidence / 'historical-eda.png')
""")
code("""rows = [{'model': name, 'pooled_wape': value['pooled_metrics']['wape'],
         'scored_rows': value['scored_rows'], 'expected_rows': value['expected_rows']}
        for name, value in result['same_original_folds_before_after'].items()]
print(pd.DataFrame(rows).to_string(index=False))
print('selected on development:', result['holdout']['selected_using_development_only'])
name = result['holdout']['selected_using_development_only']
print('holdout metrics:', result['holdout']['scores'][name]['pooled_metrics'])
display_png(evidence / 'baseline-wape.png')
""")
markdown("**Diễn giải:** Moving Average cải thiện WAPE gộp ở ba fold gốc nhưng holdout vẫn kém mốc dự báo 0. Sửa đúng công thức không bảo đảm sai số giảm cho mọi mô hình. Chọn model chỉ dùng các fold phát triển; không chọn lại sau khi xem holdout. Không suy ra hiệu quả nhập hàng hoặc tính đại diện thị trường.")
code("""from ml.training.metrics import calculate_all_metrics
print(json.dumps(calculate_all_metrics([0, 0], [4, 4]), allow_nan=False))
assert result['deployment_artifacts_unchanged'] is True
print('WAPE/bias undefined => null; original deployment artifact hashes unchanged during evaluation')
""")
notebook = {"nbformat": 4, "nbformat_minor": 5,
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                         "language_info": {"name": "python"},
                         "execution_method": "in_process_python_exec_with_captured_actual_outputs",
                         "evaluation_date": "2026-11-07"}, "cells": cells}
for i, cell in enumerate(cells): cell["id"] = f"verified-{i}"
path = ROOT / "notebooks/historical_eda_verified.ipynb"
path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1), encoding="utf-8")

# Keep the old simulator notebooks as templates, but remove unsupported findings.
for filename in ["eda.ipynb", "abc_xyz_classification.ipynb"]:
    path = ROOT / "notebooks" / filename
    nb = json.loads(path.read_text(encoding="utf-8"))
    first = ''.join(nb['cells'][0]['source'])
    note = "> Cập nhật 07/11: notebook mẫu chưa lưu output. Không dùng các nhận xét bên dưới làm kết quả thực nghiệm. Xem historical_eda_verified.ipynb cho EDA đã chạy trên snapshot của model.\n\n"
    if note not in first: nb['cells'][0]['source'] = note + first
    if filename == "eda.ipynb":
        nb['cells'][6]['source'] = "**Cần kiểm chứng sau khi chạy:** đối chiếu khóa thiếu, số dòng bị loại và miền giá trị từ output thật. Chưa có output lưu trong notebook này để xác nhận 100% dữ liệu sạch."
        nb['cells'][9]['source'] = "**Giới hạn diễn giải:** tỷ lệ kênh của simulator là giả định cấu hình, không phải ước lượng thị phần Việt Nam. Tỷ lệ hủy phải được tính từ dữ liệu của lần chạy, không ghi trước 12%."
        nb['cells'][12]['source'] = "**Cần kiểm chứng sau khi chạy:** chỉ kết luận đỉnh theo giờ hoặc tăng cuối tuần khi có biểu đồ và số đo. Các hệ số mô phỏng không chứng minh hiệu ứng thị trường thực tế."
        nb['cells'][15]['source'] = "### Kết luận và giới hạn\nNotebook này là mẫu phân tích simulator và chưa lưu kết quả chạy. Không dùng nhận xét dự kiến thay cho EDA thực nghiệm. Xem historical_eda_verified.ipynb để phân tích snapshot lịch sử cùng hạn chế zero-fill, thiếu category và coverage."
    path.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("Executed and saved five verified EDA cells; annotated legacy notebook templates.")
