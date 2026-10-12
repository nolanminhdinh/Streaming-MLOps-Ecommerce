# Lần 5 — Sửa các điểm nối end-to-end (2026-10-12)

Báo cáo tổng hợp nguyên nhân, nội dung chỉnh sửa, kiểm thử và kế hoạch nghiệm thu: [Báo cáo chỉnh sửa hệ thống](bao-cao-chinh-sua-he-thong-2026-10-12.md).

## Kết quả

Các lỗi trong [audit ngày 12/10](../03-testing-and-benchmark/e2e_audit_2026-10-12.md) đã có bản sửa và kiểm tra:

| Lỗi | Thay đổi | Bằng chứng |
|---|---|---|
| Partition Shopee/TikTok sai ngày | Parse timestamp từng dòng, chuyển Asia/Ho_Chi_Minh trước khi phân vùng | 6 regression tests đã push trong commit 6f78af7 |
| Retraining đổi nguồn | Giữ origin và fingerprints từ deployment, kiểm tra drift/train/register cùng nguồn | Tests từ chối drift cũ và model mới đổi nguồn; loader/serving đều lọc fingerprints |
| Forecast và actual khác nguồn | Forecast audit lưu origin + fingerprints; view chọn đúng fact và dataset | PostgreSQL xác nhận cùng SKU nhưng actual CSV=7, streaming=99, bỏ dataset không liên quan |
| ABC/XYZ SQL khác Python | Calendar grid gồm ngày 0 sales; Pareto dùng tích lũy trước SKU; fallback theo số lượng; CV ≤ 0,5 là X | 5 fixture SQL/Python gồm demand gián đoạn, doanh thu 0, ties, một ngày, CV=0,5; cả safety stock/ROP khớp |
| MinIO đọc lỗi bị coi là dữ liệu rỗng | Propagate lỗi để ETL dừng trước load; đóng response kể cả read lỗi | Tests lỗi storage, mất kết nối và batch có một object lỗi |

Toàn suite: **96 passed, 0 failed, 0 skipped**, 30 warnings, 9,98 giây. Warnings là 1 Starlette/httpx và 29 PyArrow/pandas deprecations. Kết quả JUnit cục bộ tại [pytest_fixes_2026-10-12.xml](../03-testing-and-benchmark/pytest_fixes_2026-10-12.xml).

Kiểm tra SQL: **8 trường hợp đạt trên PostgreSQL WASM PGlite 0.5.8 trong bộ nhớ**: upgrade schema/view cũ và rerun, 5 fixture ABC/XYZ + tồn kho, accuracy/provenance/coverage, toàn bộ query exporter trên warehouse chỉ có CSV. Test CSV trong pytest đã được tách khỏi kết nối DB thật: kiểm tra serialization, giữ actual chưa đủ dữ liệu là trống, và cho phép CSV-only. Kiểm tra SQL được chạy riêng trên engine PostgreSQL, không giả lập bằng mock.

Docker Desktop trên máy hiện lỗi khởi động với `sailor-ingest.sock`: Windows trả “The file cannot be accessed by the system”. CLI/start ứng dụng chưa tạo được Engine. Do đó **chưa áp dụng migration lên warehouse Docker, chưa rebuild API và chưa kiểm chứng toàn tuyến Kafka/MinIO/MLflow/HTTP** trong lượt này. PGlite chứng minh SQL thực thi được nhưng không thay thế nghiệm thu runtime Docker.

ETL và CSV importer cũng đã dùng parser ISO hỗn hợp giống consumer để không loại nhầm dòng có/không có phần mili giây. Script `start_full_stack.bat` dừng với exit code 1 khi Docker/Compose lỗi thay vì in các dòng OK sai. Script vẫn là khởi động hạ tầng; producer, consumer và ETL chạy riêng theo hướng dẫn.

## Áp dụng khi Docker Engine hoạt động

Tại thư mục gốc, dùng Python có dependencies dự án:

```powershell
docker compose up -d postgres
python scripts/init_warehouse.py
docker compose up -d --build fastapi
```

Migration trong `01_star_schema.sql` thêm `data_origin`, `source_datasets` vào forecast và bảng `Warehouse_Daily_Coverage`, nâng khóa unique để không gộp nguồn/model khác nhau. Forecast cũ thiếu provenance được giữ `unknown` và chưa chấm accuracy. Không suy đoán origin từ model hiện tại cho các bản ghi cũ.

Sau khi rebuild, forecast mới lưu fingerprints của feature spec model. API truy vấn history/catalog/ngày cuối từ cùng fingerprints để loại dataset không liên quan. Khi một SKU CSV chưa có trong Dim_Products, thao tác ghi forecast tạo dimension từ bảng lịch sử trong cùng transaction.

## Xác nhận ngày actual đã nạp đủ

Sự hiện diện của đơn hàng hoặc việc một batch ETL thành công chưa chứng minh đã nhận hết dữ liệu trong ngày. View chỉ tính actual/error khi ngày Việt Nam đã kết thúc **và** coverage đã được xác nhận. Nếu chưa xác nhận, `actual_complete=false`, actual/error là NULL. Khi đã xác nhận nhưng SKU không bán, actual bằng 0 là hợp lệ.

Sau khi đối soát đủ input records/Kafka lag và các batch cho một ngày, xác nhận streaming:

```powershell
python scripts/confirm_daily_coverage.py --data-origin unverified_warehouse --date 2026-10-11
```

Với CSV, phải xác nhận **từng fingerprint** đã dùng bởi model, sau khi biết nguồn xuất chứa đầy đủ ngày đó. Lấy fingerprints từ `data/feature_spec.json` hoặc `/model/metadata`/manifest mới:

```powershell
python scripts/confirm_daily_coverage.py --data-origin public_anonymized_historical_csv --source-dataset "<fingerprint>" --date 2026-08-10
```

Lệnh từ chối ngày hôm nay/tương lai theo giờ Việt Nam và dataset chưa có trong origin. Chưa có coverage thì dashboard không chấm ngày đó; đây là dữ liệu pending, không phải lỗi exporter.

## Power BI

```powershell
python powerbi/export_powerbi_dataset.py
```

Exporter cho phép warehouse chỉ chứa lịch sử CSV. File mới `Historical_Demand_Daily.csv` chứa số lượng theo SKU/ngày/platform/fingerprint. Các bảng tài chính từ streaming vẫn chỉ dùng Fact_Orders, nên không tạo doanh thu giả cho CSV không có giá trị tài chính tin cậy.

`Forecast_vs_Actual.csv` thêm `data_origin`, `source_datasets`, `actual_complete`. Refresh Power Query để nhận cột mới. Financial/inventory matrix view cũ vẫn scoped tới streaming; view mới `vw_powerbi_abc_xyz_by_origin` phục vụ phân nhóm theo từng origin.

## Drift và retraining

Tạo lại Feature Store và drift summary đúng nguồn trước trigger:

```powershell
python ml/features/feature_pipeline.py --data-origin public_anonymized_historical_csv
python monitoring/evidently/drift_detector.py
python monitoring/evidently/trigger_retraining.py
```

Drift summary cũ thiếu provenance phải được tạo lại. Retraining mặc định dùng origin/fingerprints của deployment; `--force` không cho phép đổi nguồn ngầm. Manifest cũ chưa có fingerprints chỉ được bổ sung từ feature spec cùng origin khi đọc. Manifest đăng ký mới luôn ghi fingerprints. Khi thay bộ CSV, cần train/register rõ nguồn mới thay vì để vòng retrain tự trộn các file cũ/mới. CLI retraining trả exit code 1 khi failed/blocked để automation nhận biết lỗi.

Model v2 vẫn ở Staging; metric WAPE 126,08% là kết quả cũ, chưa đánh giá lại hay nâng lên Production trong lượt này.

## Chạy lại kiểm tra SQL cô lập

Cần Node, pnpm và Python dependencies dự án. Package chỉ phục vụ kiểm thử, lưu trong `scratch/` bị gitignore:

```powershell
pnpm --dir scratch/sql-runtime add @electric-sql/pglite@0.5.8 --ignore-scripts
python tests/prepare_sql_fixtures.py scratch/sql-runtime/abc_expected.json
python tests/collect_powerbi_queries.py scratch/sql-runtime/export_queries.json
node tests/check_warehouse_sql.mjs scratch/sql-runtime/node_modules/@electric-sql/pglite/dist/index.js scratch/sql-runtime/abc_expected.json scratch/sql-runtime/export_queries.json
python -m pytest -q --tb=short
```

SQL check dùng schema cũ ở commit `6f78af7` để kiểm tra upgrade. PostgreSQL chạy trong bộ nhớ; fixtures được rollback sau từng case.

Phần sửa mã và test của lần này có hỗ trợ AI (Codex).
