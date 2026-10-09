# THƯ VIỆN TÀI LIỆU KỸ THUẬT & KIẾN TRÚC HỆ THỐNG
## Streaming MLOps E-Commerce Demand Forecasting & Dynamic Inventory Optimization

> **Tác giả**: Đinh Công Minh  
> **Chuyên ngành**: Khoa học Dữ liệu / Kỹ thuật Dữ liệu & AI — Đồ án Tốt nghiệp  
> **Phiên bản tài liệu**: 2.0 (Đã hoàn thiện & tổ chức cấu trúc module hóa)

---

## BẢN ĐỒ ĐIỀU HƯỚNG TÀI LIỆU (DOCUMENTATION ROADMAP)

Thư mục tài liệu `docs/` được phân nhóm theo từng phân hệ chức năng trong kiến trúc 6 phân tầng của hệ thống:

```
docs/
├── 00-architecture-overview/          # Tổng quan kiến trúc, slides, HTML visualizer & sơ đồ tổng thể
├── 01-ingestion-buffering/            # Tầng 1: Thu thập đơn hàng, Simulator & 3 tầng đệm Kafka/MinIO
├── 02-warehouse-etl-feature-store/    # Tầng 2 & 3: ETL/ELT, PostgreSQL Star Schema & Feature Store ML
├── 03-testing-and-benchmark/          # Kiểm thử hiệu năng, Stress Testing Locust & Soak Test 1h
├── 04-pipeline-updates/               # Nhật ký tối ưu hóa & nâng cấp pipeline qua các giai đoạn
├── 05-how-to-run/                     # Hướng dẫn thực thi theo từng tuần & Kịch bản Live Demo
└── 06-weekly-progress/                # Nhật ký tiến độ thực hiện tuần 1 đến tuần 10
```

---

## CHI TIẾT TỪNG PHÂN MỤC TÀI LIỆU

### [00-architecture-overview](00-architecture-overview/) — Tổng quan Kiến trúc Hệ thống
Chứa các tài liệu thiết kế mức cao nhất, lý thuyết công nghệ và các sơ đồ kiến trúc nền tảng:
- [`architecture.md`](00-architecture-overview/architecture.md): Tài liệu thiết kế kiến trúc hệ thống 6 phân tầng và 9 microservices.
- [`cong_nghe_ly_thuyet.md`](00-architecture-overview/cong_nghe_ly_thuyet.md): Tổng hợp cơ sở lý thuyết công nghệ (Kafka, MinIO, PostgreSQL, LightGBM, MLOps).
- [`presentation_slides.md`](00-architecture-overview/presentation_slides.md): Đề cương và nội dung slide trình chiếu bảo vệ đồ án tốt nghiệp.
- [`system_architecture_visualizer.html`](00-architecture-overview/system_architecture_visualizer.html): Ứng dụng web tương tác trực quan hóa kiến trúc hệ thống đa tab.
- **Các sơ đồ kiến trúc Vector (SVG)**:
  - [`diagram_1_tech_stack.svg`](00-architecture-overview/diagram_1_tech_stack.svg): Phân tầng công nghệ (Tech Stack Taxonomy).
  - [`diagram_2_pipeline_flow.svg`](00-architecture-overview/diagram_2_pipeline_flow.svg): Luồng dữ liệu end-to-end toàn hệ thống.
  - [`diagram_3_sequence_flow.svg`](00-architecture-overview/diagram_3_sequence_flow.svg): Sơ đồ tuần tự tương tác giữa các dịch vụ (Sequence Diagram).
  - [`diagram_4_plug_and_play.svg`](00-architecture-overview/diagram_4_plug_and_play.svg): Ranh giới cắm-và-chạy (Source-Agnostic Plug-and-Play Boundary).

---

### [01-ingestion-buffering](01-ingestion-buffering/) — Thu thập Sự kiện & Đệm Dữ liệu Thời gian thực
Tập trung vào phân tầng 1 và cơ chế chống tràn, lệch pha tốc độ:
- [`thiet_ke_buoc_dem_hung_du_lieu.md`](01-ingestion-buffering/thiet_ke_buoc_dem_hung_du_lieu.md): Báo cáo thiết kế kỹ thuật **3 Tầng Đệm Hứng Dữ Liệu** (Producer Buffer $\rightarrow$ Kafka Broker Log $\rightarrow$ Consumer Micro-batching Buffer $\rightarrow$ MinIO Parquet).
- [`bao_cao_xay_dung_data_simulator.md`](01-ingestion-buffering/bao_cao_xay_dung_data_simulator.md): Báo cáo thiết kế bộ sinh dữ liệu mô phỏng chuẩn xác Shopee (84 cột) và TikTok Shop (71 cột).
- **Sơ đồ Vector (SVG)**:
  - [`diagram_5_three_tier_buffer.svg`](01-ingestion-buffering/diagram_5_three_tier_buffer.svg): Kiến trúc chi tiết 3 tầng đệm dữ liệu thời gian thực.

---

### [02-warehouse-etl-feature-store](02-warehouse-etl-feature-store/) — Kho Dữ liệu & Kỹ nghệ Đặc trưng ML
Tập trung vào chu trình xử lý dữ liệu sau bước đệm và chuẩn bị cho mô hình AI:
- [`bao_cao_etl_va_feature_engineering.md`](02-warehouse-etl-feature-store/bao_cao_etl_va_feature_engineering.md): Báo cáo thiết kế kỹ thuật và kiểm thử thực nghiệm:
  - Chu trình **ETL/ELT** (Extract $\rightarrow$ Unify Schema $\rightarrow$ Clean $\rightarrow$ Data Quality Validation $\rightarrow$ Load Star Schema).
  - **Mô hình Dữ liệu Chấm sao (Star Schema)**: Fact_Orders và 5 bảng Dimension.
  - **Kỹ nghệ đặc trưng chuỗi thời gian**: Gom nhóm ngày (Daily Aggregation), Lưới Descartes lấp đầy ngày khuyết (Cartesian Grid), Ma trận ABC/XYZ, Lags, Rolling Statistics với nguyên tắc Shift(1) chống Data Leakage tuyệt đối.
  - **Walk-Forward Validation**: Phân chia tập dữ liệu huấn luyện theo Cửa sổ mở rộng (Expanding Window).
- [`bao_cao_xay_dung_power_bi_dashboard.md`](02-warehouse-etl-feature-store/bao_cao_xay_dung_power_bi_dashboard.md): Báo cáo thiết kế kỹ thuật **Executive Dashboard & Mô hình hóa Chiều theo Chuẩn Ralph Kimball** (The Data Warehouse Toolkit):
  - Phương pháp luận thiết kế chiều: Conformed Dimensions (`Dim_Dates`, `Dim_Products`), Star Joins và cơ chế Drill-Across.
  - Phân hệ 19 công thức quản trị DAX nhóm trong 3 Display Folders của bảng `_Measures`.
  - Bộ Theme màu Pastel doanh nghiệp và chiến lược vận hành 2 chế độ (Flat CSV Data Marts vs DirectQuery PostgreSQL).
- [`data_dictionary_data_model_specification.md`](02-warehouse-etl-feature-store/data_dictionary_data_model_specification.md): **Tài liệu Quản lý Dự án Báo cáo, Từ điển Dữ liệu & Đặc tả Mô hình Dữ liệu** (Project Management Specification, Data Dictionary & BI Model):
  - Bảng 1: Nhật ký tinh chỉnh kỹ thuật, logic nghiệp vụ & truy vấn SQL (Task & SQL Change Log).
  - Bảng 2: Từ điển dữ liệu toàn diện 7 bảng (`Dim_Dates`, `Dim_Products`, `Dim_Geography`, `Dim_Shops`, `Fact_Orders_Summary`, `Forecast_vs_Actual`, `Inventory_Health_Alerts`).
  - Phần 3: Bản đồ nghiệp vụ phân tích báo cáo (Business Mindmap 3 trụ cột).
  - Phần 4: Mô hình dữ liệu Chấm sao chuẩn hóa Ralph Kimball (Star Schema Data Model).
  - Bảng 5: Danh mục 3 trang báo cáo Power BI (Executive Overview, Demand Forecasting, Inventory Risk Matrix).
  - Bảng 6: Thư viện 19 công thức quản trị DAX chuẩn hóa (Measures & DAX Formulas).
  - Bảng 7: Quy trình & Tiến độ triển khai 10 tuần (Project Timeline).
  - *Tệp bảng tính Excel đính kèm đầy đủ 7 sheets*: [`data_management_specification.xlsx`](02-warehouse-etl-feature-store/data_management_specification.xlsx) và tệp CSV [`data_dictionary.csv`](02-warehouse-etl-feature-store/data_dictionary.csv).
- **Các sơ đồ Vector (SVG)**:
  - [`diagram_6_etl_star_schema_pipeline.svg`](02-warehouse-etl-feature-store/diagram_6_etl_star_schema_pipeline.svg): Luồng xử lý chi tiết ETL/ELT từ MinIO vào PostgreSQL.
  - [`diagram_7_star_schema_er_model.svg`](02-warehouse-etl-feature-store/diagram_7_star_schema_er_model.svg): Sơ đồ quan hệ thực thể (ERD) Star Schema.
  - [`diagram_8_feature_engineering_flow.svg`](02-warehouse-etl-feature-store/diagram_8_feature_engineering_flow.svg): Luồng kỹ nghệ đặc trưng và xuất Feature Store.
  - [`diagram_9_walk_forward_validation.svg`](02-warehouse-etl-feature-store/diagram_9_walk_forward_validation.svg): Minh họa cơ chế phân chia Walk-Forward Validation Expanding Window.

---

### [03-testing-and-benchmark](03-testing-and-benchmark/) — Kiểm thử Hiệu năng & Thử nghiệm Tải
- [`bao_cao_kiem_thu_hieu_nang_lan_1.md`](03-testing-and-benchmark/bao_cao_kiem_thu_hieu_nang_lan_1.md): Báo cáo thực nghiệm hiệu năng hệ thống đợt 1.
- [`bao_cao_tong_hop_hieu_nang_toan_he_thong.md`](03-testing-and-benchmark/bao_cao_tong_hop_hieu_nang_toan_he_thong.md): Báo cáo tổng hợp kiểm thử chịu tải (Locust 10-200 RPS) và Soak Test liên tục 1 giờ.

---

### [04-pipeline-updates](04-pipeline-updates/) — Nhật ký Tối ưu Hóa
- [`lan-01-khac-phuc-loi-va-toi-uu-luong.md`](04-pipeline-updates/lan-01-khac-phuc-loi-va-toi-uu-luong.md): Chi tiết các lần tinh chỉnh tham số batch size, socket timeout, và cơ chế flush buffer.
- [`README.md`](04-pipeline-updates/README.md): Tổng kết các mốc cập nhật pipeline.

---

### [05-how-to-run](05-how-to-run/) — Hướng dẫn Vận hành & Live Demo
- Hướng dẫn cài đặt môi trường và chạy thử nghiệm từng tuần từ Tuần 2 đến Tuần 10 (`how-to-run-week*.md`).
- [`live_demo_script.md`](05-how-to-run/live_demo_script.md): Kịch bản chuẩn bị và trình diễn demo trực tiếp trước hội đồng chấm thi.

---

### [06-weekly-progress](06-weekly-progress/) — Nhật ký Tiến độ Tuần
- Ghi chép chi tiết tiến độ nghiên cứu, cài đặt và thực nghiệm từ `tuan-01.md` đến `tuan-10.md`.
