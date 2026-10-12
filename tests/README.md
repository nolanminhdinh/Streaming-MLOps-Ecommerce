# Tests

- `test_data_simulator.py`: unit test cho logic sinh dữ liệu.
- `test_ingestion.py`: partition Shopee/TikTok theo ngày Việt Nam.
- `test_etl.py`, `test_features.py`: chuẩn hóa, validation, đặc trưng và ABC/XYZ.
- `test_pipeline_integrity.py`: lỗi MinIO, provenance drift/retrain/forecast, daily coverage, export warehouse rỗng.
- `test_serving.py`, `test_serving_realdata.py`: API, inventory, feature parity; repository giả cho unit tests.
- `test_load_test.py`: benchmark in-process và CSV export với warehouse query fixtures.
- `load_testing/`: kiểm thử HTTP/load runtime riêng.
- `check_warehouse_sql.mjs`: PostgreSQL cô lập, migration/view compatibility, SQL/Python parity và forecast scoring đúng nguồn.

Chạy `python -m pytest -q --tb=short`. Cách chạy PostgreSQL cô lập được ghi tại [Lần 5](../docs/04-pipeline-updates/lan-05-pipeline-integrity-2026-10-12.md). Test suite không xác nhận Kafka, MinIO, MLflow và API Docker thật đang chạy.
