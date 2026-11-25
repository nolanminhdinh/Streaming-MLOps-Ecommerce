# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Bối cảnh dự án

Đồ án tốt nghiệp (ĐATN) ngành Khoa học Dữ liệu: hệ thống pipeline dữ liệu luồng + MLOps dự báo nhu cầu hàng hóa thương mại điện tử (Shopee / TikTok Shop, Việt Nam). Tài liệu, README, comment và báo cáo viết bằng **tiếng Việt**; tài liệu mới cũng viết bằng tiếng Việt, xưng "em" khi viết thay sinh viên trong báo cáo đồ án.

`PROJECT_CONTEXT.md` là nguồn sự thật về công thức, schema và lịch sử theo tuần. Các ghi chú có ngày ở đầu tệp ghi đè mô tả cũ hơn bên dưới; luôn đọc ghi chú mới nhất trước. Ghi chú trong tệp này mới hơn `PROJECT_CONTEXT.md` (tệp đó chưa được cập nhật sau 08/11/2026). `docs/05-how-to-run/core-pipeline.md` là hướng dẫn vận hành hiện hành.

## Trạng thái hiện tại (21/11/2026)

- **Commit:** ngày 23/11 đã commit và push lên `origin/main` (`2417911`) phần mã, test, tài liệu, PBIP (không có `cache.abf`) và bằng chứng nhỏ trong `output/`. Không commit (giữ trên máy): `output/bao-cao-do-an-*` (báo cáo Word/PDF, ảnh minh chứng AI), `output/docx/`, `output/models/`, `tmp/`, `Tai_lieu_chat_DATN_2026-11-08*`, `.tools/`, `.audit-e2e/`, mọi tệp `*.zip`. Hỏi sinh viên trước mỗi lần commit.
- **Stack đang tắt** bằng `docker compose --profile demo stop`; volume còn nguyên.
- **Pipeline:**
  - đã kiểm chứng end-to-end;
  - chạy lại cùng dữ liệu 20/20;
  - thử lỗi hạ tầng 3/3;
  - chạy liên tục 1 giờ ngày 19–20/11: 3.318 bản tin Kafka = 3.318 dòng kho thô = 3.318 dòng `Fact_Orders`; độ trễ nhận → kho p95 khoảng 41 giây; stack dùng khoảng 3 GB RAM và 1,45 lõi CPU;
  - thử tải cao điểm YC7 ngày 22/11: 12 sự kiện/giây trong 20 phút, 14.380 bản tin = 14.380 dòng `Fact_Orders` = 14.380 dòng mart (mã lượt `R172444`), p95 33–46 giây, lag về 0 sau 67 giây. Bằng chứng `output/peak-load-2026-11-22/`; bộ phát `scripts/peak_load_producer.py` (gọi qua `check_streaming_soak.py --fixed-rate 12`) vì `producer.py` chờ ack từng bản tin và nghỉ 1 giây mỗi đợt, chỉ đạt khoảng 7 sự kiện/giây.
- **Kiểm thử:** 261 test đạt; 54 test PostgreSQL opt-in và 11 test mart đạt.
- **Model phục vụ:** Naive v4 (Staging), huấn luyện ngày 16/11 trên `warehouse_live`. WAPE backtest 58,22%, bias −23,07%; mô hình học máy chưa vượt baseline (giới hạn đã công bố).
- **Power BI:**
  - PBIP 6 trang tại `powerbi/BaoCaoQuanLy/`; số liệu đã đối chiếu khớp SQL ngày 17/11.
  - **Sinh viên tự chỉnh giao diện trong Power BI Desktop**: không chạy lại `scripts/build_powerbi_project.py` nếu chưa hỏi, vì script ghi đè phần sinh viên đã sửa.
  - Ngày 22/11 (tối), theo yêu cầu sinh viên, định dạng PBIP được chỉnh tại chỗ theo mẫu Power BI sinh viên gửi (`~/Downloads/bao_cao.pbix`, báo cáo "Customer 360" của KPIM, không lưu vào repo):
    - dải đầu trang navy có logo; tiêu đề in hoa; slicer chuyển lên dải đầu trang;
    - thẻ KPI có icon SVG tự vẽ;
    - khung visual trắng có thanh tiêu đề màu `#768CCE`, đổ bóng nhẹ;
    - nền trang sáng tự vẽ.
    - Bảng màu theo yêu cầu sinh viên là theme "Pastel Elegance" cũ của dự án (`#768CCE`, `#FFC6D0`, `#C3D6F2`…, chữ `#2E3A59`), ghi vào `QuanLyKho.json`. Màu trạng thái đỏ/cam/xanh lá của measure `Status Color` giữ nguyên.
    - Không đổi truy vấn, measure, bộ lọc hay model (đã đối chiếu với bản sao lưu). `build_powerbi_project.py` chưa có phần định dạng này, nên chạy lại script cũng làm mất nó.
  - Ngày 22/11 (khoảng 19h15), theo yêu cầu sinh viên, tiêu đề visual được rút gọn (bỏ ghi chú trong ngoặc). Có thêm 30 nút ⓘ dạng actionButton `information`, chỉ có tooltip, giống cách mẫu KPIM làm:
    - 19 nút ngay sau tiêu đề biểu đồ/bảng;
    - 6 nút ở góc phải hàng thẻ KPI;
    - 5 nút cạnh bộ lọc ở dải đầu trang (trang Độ tin cậy không có bộ lọc).
    - Nội dung tooltip gồm cách đọc, công thức chỉ số, lưu ý và cách lọc, lấy theo measure và SQL `bi` thật.
    - Tooltip của nút không xuống dòng được nên các ý tách bằng "•".
    - Khung "Cách đọc" ở trang Dự báo giữ nguyên.
  - Ngày 22/11 (khoảng 19h40), theo yêu cầu sinh viên:
    - **Bảng và ma trận:** cỡ chữ lớn hơn; độ rộng cột đặt cứng (`columnWidth`) cho vừa khung; tăng `rowPadding`. Ma trận ABC × XYZ chia độ rộng cho 3 cột giá trị đang có dữ liệu (Y, Z, Total); nếu sau này xuất hiện cột X thì có thể phải cuộn ngang.
    - **Biểu đồ cột và thanh:** giảm `innerPadding` cho cột to hơn và bật nhãn dữ liệu. "Doanh thu theo ngày" và "Số đơn theo ngày và trạng thái xác nhận" đổi từ cột cụm sang cột chồng (`columnChart`), có nhãn tổng.
    - **Tiền:** 6 measure tiền dùng `formatStringDefinition`, trả về chuỗi hằng đã chia đơn vị bằng `FORMAT` theo culture vi-VN ("83,0 tỷ ₫", "603 nghìn ₫"). Power BI không hỗ trợ chia đơn vị bằng dấu phẩy trong format string, và hậu tố K/bn theo ngôn ngữ ứng dụng chứ không theo culture của model. Vì vậy các visual có tiền đặt `labelDisplayUnits` = 1 (không rút gọn), và biểu đồ "Doanh thu theo ngày" ẩn trục giá trị.
  - Ngày 22/11 (khoảng 22h40), theo yêu cầu sinh viên, biểu đồ thanh "Số ngày tồn của SKU cần đặt" (trang 1) đổi thành "Tồn kho so với điểm đặt hàng": mỗi SKU 2 thanh (`Stock On Hand Today` và measure mới `Reorder Point Today`), xếp theo điểm đặt hàng giảm dần (khung chỉ hiện ~5 SKU, phải cuộn; sinh viên đã đồng ý giữ). Bảng "Các phiên bản model đã phát dự báo" giảm `rowPadding` từ 7 xuống 2 để hiện đủ 4 phiên bản. Lý do đổi biểu đồ: 19/20 SKU cần đặt có tồn 0 sau các lượt thử tải (ảnh chụp tồn kho 22/11 00:44), nên biểu đồ số ngày tồn chỉ có 1 thanh. Model giờ có 64 measure.
  - Ngày 22/11, PBIP đã được Refresh và Save trong Desktop: có `BaoCaoQuanLy.SemanticModel/.pbi/cache.abf` (khoảng 22 MB, bị `.gitignore` của PBIP bỏ qua), nên mở ra là có số mà không cần PostgreSQL. Khi Save, chọn "Don't upgrade" (giữ `model.bim`, không chuyển sang TMDL).
  - Ngày 22/11, sinh viên yêu cầu xóa các tệp Power BI không dùng: 2 PBIX cũ `powerbi/Report_PowerBI*.pbix`, 6 bản sao PBIX trong `Tai_lieu_chat_DATN_2026-11-08/` và `tmp/`, 7 tệp `theme_pastel.json`. `powerbi/data/*.csv` và `powerbi/import/*.pq` vẫn được pipeline, demo-web và bước kiểm tra Refresh dùng. `build_powerbi_project.py` giờ lấy base theme từ PBIP hiện có; `check_powerbi_readonly.py` bỏ qua phần PBIX cũ.
- **Báo cáo đồ án (21/11/2026): đã dựng lại theo nhận xét của GVHD**
  - Bản hiện hành: `output/bao-cao-do-an-2026-11-21/Bao-cao-DATN-dinh-dang-theo-mau-2026-11-21.docx` (117 trang, kèm PDF). Ngày 22/11 (chiều): tài liệu tham khảo bỏ "Truy cập …", DOI đổi thành link https://doi.org/…, thêm link cho [3] GitHub, [10] Kafka, [14] LightGBM; mọi URL là hyperlink bấm được (đã kiểm tra từng link). Sửa tại chỗ ngày 22/11: kết quả YC7 vào Bảng 2-1, mục 2.5, Bảng KL-1/KL-2/KL-3; chụp lại Hình 4-3 (MLflow, trước đó bị hộp thông báo che); cắt thanh tác vụ Windows khỏi 11 ảnh chụp màn hình. Sinh viên muốn ảnh chụp màn hình **chỉ chụp cửa sổ ứng dụng/trình duyệt**, không chụp cả màn hình, và không có hộp thông báo che nội dung (chụp bằng `PrintWindow`, đóng popup trước khi chụp). Bản sinh viên lưu có thể do trình soạn khác Word lưu (có `customXml`, ảnh bị nén), nên luôn so với bản mới nhất trước khi ghi. Nội dung giữ nguyên như bản `…-bo-cuc-moi-…`, chỉ đổi định dạng theo báo cáo mẫu mà sinh viên gửi (báo cáo của một sinh viên khác, không lưu vào repo). Nguồn dựng lại ở `nguon-dinh-dang-mau/` (`build2.py`).
  - **Định dạng theo báo cáo mẫu:**
    - Chữ Times New Roman 13, giãn dòng 1,3, trước đoạn 6 pt, thụt đầu dòng 1 cm.
    - Lề A4: trái 3,5 cm, trên và dưới 3 cm, phải 2 cm.
    - Hai trang bìa có khung viền: bìa ngoài có gáy chữ dọc, bìa trong có GVHD và sinh viên. "Khóa 67" là suy đoán từ mã sinh viên.
    - Phần đầu đánh số trang La Mã; thân bài đánh số Ả Rập từ Mở đầu.
    - "CHƯƠNG n" và các mục 1.1, 1.1.1 đánh số tự động.
    - Chú thích "Hình 2-1 …" không in đậm.
    - Tài liệu tham khảo chia ba nhóm (tiếng Việt, tiếng Anh, Internet) và đánh số theo nhóm.
    - Phụ lục đánh "Phụ lục 1–6", không đưa vào mục lục. Phụ lục 5 có 18 ảnh "Minh chứng n" (làm lại ngày 23/11, xem ghi chú bên dưới), trích nguyên văn lịch sử AI.
    - Footer: "Sinh viên: … – Khoa Công Nghệ Thông Tin – Khóa 67" kèm số trang.
  - Bản `Bao-cao-DATN-bo-cuc-moi-2026-11-21.docx` (96 trang) là bản trước khi đổi định dạng. Bản ở `output/bao-cao-do-an-2026-11-17/` đã lỗi thời.
  - Đối chiếu nhận xét → cách xử lý, mục cũ → mục mới: `doi-chieu-bo-cuc-va-nhan-xet.md` cùng thư mục.
  - Nguồn dựng lại: thư mục `nguon/` (nội dung `content/*.txt` dạng đánh dấu, `build.py`, `gen_diagrams.py`). Dựng lại bằng `python build.py out.docx`, rồi chạy `docx2pdf.ps1 -UpdateToc` để Word cập nhật mục lục. Sơ đồ PNG ở `so-do/`.
  - Việc còn lại của sinh viên:
    - điền lớp, hệ đào tạo, điện thoại, email ở Thông tin đồ án;
    - Phụ lục 5 đã có 18 ảnh minh chứng trích từ lịch sử AI; nếu cần, thêm ảnh chụp giao diện thật của các công cụ AI;
    - đọc lại văn phong, kiểm tra Bảng AI-1;
    - thay ảnh Power BI sau khi chỉnh giao diện.

## Quyết định của sinh viên (phải tuân theo)

**Phạm vi (08/11/2026)**
- Ưu tiên cao nhất là **pipeline dữ liệu** end-to-end chạy đúng: kiểm soát chất lượng, chống trùng, phục hồi, nạp gia tăng, truy vết, đầu ra hạ nguồn.
- ML là nhánh tiêu thụ dữ liệu để minh họa MLOps; không tự tối ưu độ chính xác mô hình khi chưa được yêu cầu. WAPE cao là giới hạn được công bố, không phải lỗi pipeline.
- Báo cáo vẫn phải có so sánh mô hình bằng WAPE/bias theo walk-forward, với Naive làm baseline (Cẩm nang nhóm 4 yêu cầu).

**NiFi**
- NiFi nằm ngoài phạm vi: đã tách sang `docker-compose.nifi.yml`, không thuộc tiêu chí nghiệm thu.
- Container `nifi` cũ có restart policy nên Docker Desktop hay tự bật lại. Khi đo tài nguyên hoặc chạy test, kiểm tra và `docker stop nifi`.

**Lưu trữ (16/11/2026)**
- MinIO tăng theo số lần nhận và `checkpoint.json` lưu toàn bộ `completed_objects`. Đây là **giới hạn giữ lại có chủ đích cho demo**, làm bằng chứng phản biện.
- Không tự thêm lifecycle, compaction hay đổi checkpoint; hướng giải quyết ghi ở `docs/04-pipeline-updates/lan-12-…`.
- Cùng giới hạn này làm demo-web tốn khoảng 0,7 lõi CPU: cứ 4 giây nó liệt kê toàn bộ object. Chỉ ghi nhận, không sửa.

**Dữ liệu (16/11/2026)**
- Toàn bộ dữ liệu trong kho là mô phỏng hoặc kiểm thử. Không gắn cờ hay lọc riêng các lô có nhãn (`RPT`, `AUTO`, `FLT`, `E2E`, `CMP`, `DI`, `WEB…`).
- Không thêm cờ ngày lương hoặc ngày lễ vào `Dim_Dates`, vì ngày lương khác nhau theo công ty.

**Báo cáo Power BI (16/11/2026)**
- Có 4 trang chính: Tồn kho, Dự báo AI, Đặt hàng nhà cung cấp, Đơn hàng từ các sàn; cộng trang Chi tiết SKU và trang Độ tin cậy dữ liệu.
- Đặc tả: `powerbi/dac-ta-bao-cao-quan-ly.md`.
- Đề xuất đặt hàng chỉ lấy từ `bi.replenishment_recommendation`, mức phục vụ A 98% / B 95% / C 90%.
- **Hàng đang về là hướng mở rộng**: `incoming_stock` và `restock_eta` được thu nhưng không đưa vào công thức.
- Data mart dùng **bảng vật lý** trong schema `mart`: giữ lịch sử đề xuất theo ngày, ghi phiên bản model của từng dự báo, khóa ngày dạng yyyymmdd.
- Khóa thay thế: −1 nghĩa là không xác định; −2 nghĩa là không áp dụng (`dim_carrier`, `dim_model`).
- Hai tệp PBIX cũ `powerbi/Report_PowerBI*.pbix` đã bị xóa theo yêu cầu sinh viên ngày 22/11; báo cáo duy nhất là PBIP `powerbi/BaoCaoQuanLy/`.

**Model (16/11/2026)**
- Sinh viên đã duyệt huấn luyện lại trên `warehouse_live`.
- Ngoài lần đó, không thay model đang phục vụ, `data/model_manifest.json`, feature snapshot hoặc trạng thái Registry như một tác dụng phụ.

**Mã đơn (17/11/2026, chọn "phương án A")**
- Không dùng quy tắc "giữ bản ghi đầu tiên".
- Cùng mã đơn và cùng `create_time` là cập nhật trạng thái: bản mới nhất thắng.
- Cùng mã nhưng khác `create_time` là xung đột danh tính: đơn đến trước giữ mã, dòng còn lại bị cách ly với lý do `ORDER_IDENTITY_CONFLICT`.
- Producer gắn mã lượt chạy `R<HHMMSS>` vào mã đơn.
- 358 đơn bị gộp trước đó đã được khôi phục từ kho thô với mã `<mã gốc>-HHMMSS`.

**Báo cáo trung thực**
- Phân biệt bằng chứng runtime đã kiểm chứng với kế hoạch.
- Không mô tả hệ thống là tự động hoàn toàn end-to-end: training, đăng ký model, drift và retraining là tác vụ kích hoạt thủ công.
- Khi đưa ảnh vào báo cáo, ghi đúng loại ảnh: ảnh desktop thật, ảnh Chrome headless, trang kết quả lệnh do script dựng, hay biểu đồ vẽ từ số đo.

**Container ngoài dự án**
- Các container CVAT (`cvat_*`, `nuclio-*`) không thuộc dự án. Ngày 19/11 sinh viên cho phép dừng vì chưa dùng.
- Chỉ dùng `docker stop`, không xóa.

## Lệnh thường dùng

```powershell
# Cài đặt (Windows, Python venv)
python -m venv .venv; .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env   # chỉ khi chưa có .env

# Kiểm thử. pytest.ini giới hạn testpaths=tests (các thư mục bản sao tài liệu chứa test trùng tên)
# và tự nạp tests/pipeline_evidence_plugin.py (fixture case_data; chỉ ghi evidence khi có --pipeline-evidence-dir).
# tests/conftest.py đặt SERVING_USE_WAREHOUSE=false và bỏ MLFLOW_TRACKING_URI.
python -m pytest -q --tb=short
python -m pytest tests/test_etl.py::test_name -q
$env:RUN_PIPELINE_COMPLETION_PG="1"; python -m pytest tests/test_pipeline_completion_postgres.py -q   # PostgreSQL thật, chỉ rollback
$env:RUN_MART_TESTS="1"; python -m pytest tests/test_mart.py -q

# Stack (13 container; demo-web thuộc profile demo)
docker compose --profile demo up -d
docker compose --profile demo ps -a
docker compose logs --tail 100 pipeline-worker
docker compose --profile demo stop      # tắt mà giữ dữ liệu
python scripts/init_warehouse.py        # khởi tạo warehouse, không xóa dữ liệu
python -m warehouse.reporting.mart --full   # làm mới toàn bộ data mart (sau migration sửa dữ liệu public)

# Đưa dữ liệu vào và kiểm tra
python ingestion/producer.py
python scripts/check_continuous_pipeline.py   # kiểm tra E2E có nhãn
python scripts/check_replay_determinism.py    # gửi cùng bộ dữ liệu hai lần, so kết quả
python scripts/check_fault_recovery.py        # thử ngừng PostgreSQL / MinIO / kill worker
python scripts/check_etl_warehouse_readonly.py --audit-date YYYY-MM-DD --source-date YYYY-MM-DD --runtime   # đối soát kho thô ↔ kho
python scripts/backfill_pipeline.py --from-date YYYY-MM-DD --to-date YYYY-MM-DD [--objects-file f.json] [--apply]
python scripts/check_streaming_soak.py --minutes 60 --interval 30 --out output/<thư mục>   # chạy bền: CPU/RAM, lag, độ trễ, đối soát
python scripts/render_soak_report.py output/<thư mục>          # biểu đồ tài nguyên và vận hành từ kết quả chạy bền
python scripts/capture_service_screenshots.py --out <thư mục> --tag <nhãn> [--soak-dir <thư mục>] [--only 02-,07-]   # ảnh Chrome headless từng service
python scripts/repair_collided_order_ids.py [--apply]          # khôi phục đơn bị gộp do trùng mã (mặc định diễn tập)

# Power BI
python scripts/check_powerbi_refresh_readiness.py --label <nhãn>   # mô phỏng Refresh (chỉ đọc), phải PASS trước demo
python scripts/check_powerbi_kpi_values.py --cutoff "YYYY-MM-DD HH:MM:SS"   # tính lại số của báo cáo bằng SQL để đối chiếu
python scripts/build_powerbi_project.py   # sinh lại PBIP (GHI ĐÈ giao diện sinh viên đã chỉnh; hỏi trước)

# ML (tác vụ thủ công; cũng chạy được qua profile compose `trainer`)
python ml/features/feature_pipeline.py --data-origin warehouse_live
python ml/training/train_baseline.py --experiment-name demand-forecasting-baseline --data-origin warehouse_live
python ml/training/compare_models.py
python ml/training/register_model.py --experiment-name demand-forecasting-baseline --stage Staging
```

- Code `serving/` được đóng vào image `fastapi` (chỉ mount `data/`), nên sau khi sửa serving phải chạy `docker compose up -d --build fastapi`.
- `pipeline-worker` và `demo-web` mount mã nguồn trực tiếp nhưng vẫn cần restart để nạp lại module Python.
- Không bao giờ dùng `docker compose down -v`, vì lệnh này xóa volume Kafka, ZooKeeper và Postgres.
- Không chạy consumer hoặc ETL thủ công khi `pipeline-worker` đang chạy: Kafka group chia lại partition làm hỏng đối soát.
- Trước khi tắt stack, chờ hai group `minio-writer-group` và `inventory-postgres-writer-group` về lag 0 và không còn object chờ (`data/pipeline/status.json`). Group `nifi-ecom-orders-consumer` có lag là bình thường, bỏ qua.

## Kiến trúc

```
data_simulator → ingestion/producer.py → Kafka (ecom.orders.raw, inventory.logs)
  ecom.orders.raw → ingestion/consumer_to_minio.py → MinIO raw/orders (Parquet, phân vùng theo ngày giờ Việt Nam)
                  → warehouse/etl (extract → validate/contract → identity → transform → load) → PostgreSQL public
  inventory.logs  → ingestion/consumer_inventory_to_postgres.py → PostgreSQL public
  bản ghi lỗi     → MinIO quarantine/orders | quarantine/inventory (kèm lý do)
PostgreSQL public → view bi → data mart mart (bảng vật lý) → Power BI PBIP (Import)
PostgreSQL → ml/features → ml/training → MLflow Registry → serving/app (FastAPI) ← batch-forecaster (dự báo 14 ngày theo lô)
FastAPI /metrics + telemetry pipeline → Prometheus → Grafana; Prometheus → Alertmanager → receiver nội bộ (:9094)
demo-web (FlowLab, :8050) đọc trạng thái các tầng để demo
```

Các điểm xuyên suốt quan trọng:
- **`scripts/run_pipeline_worker.py`** (service `pipeline-worker`):
  - giám sát hai consumer và chạy `warehouse.etl.pipeline.run_etl_pipeline` trên các object MinIO mới, chu kỳ 30 giây;
  - checkpoint chỉ tiến khi ETL thành công;
  - sau mỗi chu kỳ thì tự xác nhận coverage và làm mới mart;
  - khi khởi động thì áp migration và cài schema `bi` và `mart`;
  - ghi heartbeat vào `data/pipeline/status.json`, dùng cho healthcheck Docker;
  - khóa advisory PostgreSQL (`warehouse/etl/coordination.py`) dùng chung với backfill và các migration.
- **Ngữ nghĩa phân phối at-least-once:**
  - consumer chỉ commit offset Kafka sau khi ghi MinIO hoặc Postgres thành công;
  - replay an toàn nhờ upsert theo khóa dòng đơn và phiên bản sự kiện nguồn (`warehouse/ddl/03_event_versions.sql`, `04_integrity_constraints.sql`);
  - snapshot tồn kho cũ không ghi đè snapshot mới;
  - xung đột danh tính mã đơn: `warehouse/etl/identity.py` (xem quyết định 17/11).
- **Hợp đồng dữ liệu:** `warehouse/contracts/orders-v1.json` và `warehouse/etl/contracts.py`. Schema thay đổi phá vỡ thì cách ly (`quarantine.py`), không ép về giá trị mặc định. Lineage và đối soát theo từng object, từng lượt chạy nằm trong `warehouse/etl/observability.py`.
- **Schema:**
  - `public` là star schema tích hợp (`warehouse/ddl/01_star_schema.sql`), giữ tương thích cho consumer và ETL;
  - `bi` là các view quyết định (`warehouse/reporting/bi_schema.sql`);
  - `mart` là nguồn duy nhất của Power BI.
- **Serving** (`serving/app/main.py`):
  - `/health` nghĩa là tiến trình còn sống;
  - `/ready` chỉ trả 200 khi model ML và lịch sử warehouse sẵn sàng; serving dự phòng không được coi là "ready";
  - các endpoint khác: `/predict/demand`, `/predict/batch`, `/inventory/reorder-alert`, `/model/reload`, `/model/metadata`, `/training/status`, `/pipeline/status`, `/metrics`;
  - nạp model qua MLflow Registry và `data/model_manifest.json`; `features.py` phải khớp `ml/features` (có test).
- **Quy ước đánh giá dự báo:**
  - nhãn là t+1 tính từ origin t;
  - WAPE/bias trả `None` khi tổng actual bằng 0;
  - chọn model theo WAPE gộp (protocol `origin_t_plus_1_pooled_wape_v2`);
  - đánh giá nhiều ngày là đệ quy theo từng SKU;
  - `serving/app/sparse_demand.py` chứa ứng viên nhu cầu thưa offline (SBA/TSB/Hurdle), chưa nối vào serving.
- **Coverage và nguồn huấn luyện:**
  - Worker tự xác nhận ngày vào `Warehouse_Daily_Coverage` (`data_origin='warehouse_live'`, `warehouse/etl/coverage.py`) khi đủ bốn điều kiện: ngày đã qua, không còn object chờ, lag Kafka bằng 0, khối lượng ≥ 20% trung vị 28 ngày.
  - Ngày khối lượng thấp bị giữ lại để xác nhận tay.
  - Model huấn luyện trên `warehouse_live`; `register_model.py` chặn `unverified_warehouse`.
  - Serving lấy ngày cuối lịch sử là ngày đã xác nhận gần nhất.
- **Data mart `mart`:**
  - DDL ở `warehouse/ddl/05_mart_star_schema.sql`; hàm làm mới `mart.refresh(p_full)` ở `warehouse/reporting/mart_refresh.sql`.
  - Có 9 bảng chiều và 5 bảng fact: `fact_sales_line`, `fact_sales_order` (snapshot tích lũy), `fact_inventory_daily`, `fact_demand_forecast` (mọi phiên bản), `fact_replenishment_daily` (không bao giờ bị TRUNCATE).
  - Làm mới gia tăng theo `loaded_at` và `forecast_id`, chồng lấn 10 phút, có khóa advisory.
  - Các cột thời gian Power BI nạp lưu dạng TIMESTAMP giờ Việt Nam: connector PostgreSQL của Power BI lỗi với TIMESTAMPTZ.
- **Power BI PBIP** do `scripts/build_powerbi_project.py` sinh ra: 15 bảng, 23 quan hệ, 63 measure, 6 trang (bản PBIP hiện tại có thêm `Reorder Point Today` thêm tay ngày 22/11, script chưa có).
  - Cột sắp xếp phải tạo trong Power Query, không dùng cột tính DAX, để tránh lỗi vòng phụ thuộc.
  - Thẻ "WAPE thực tế (t+1)" chỉ tính model đang phục vụ ở tầm 1 ngày.
  - Trang Chi tiết SKU chỉ hiện số khi chọn đúng 1 SKU.
- **Dự báo theo lô:** `batch-forecaster` (`scripts/run_batch_forecast.py`) gọi `/predict/batch` 14 ngày khi `history_end` hoặc phiên bản model đổi, rồi ghi `Model_Serving_Snapshot`.
- **SQL chạy qua `exec_driver_sql`** (kể cả `bi_schema.sql` và `mart_refresh.sql`) **không được chứa ký tự phần trăm**, kể cả trong comment, vì psycopg2 coi đó là tham số. Dùng `CHR(37)`.
- **Múi giờ nghiệp vụ** là `Asia/Ho_Chi_Minh` (`BUSINESS_TZ`). Mã đơn của simulator lấy ngày và giờ theo UTC; ngày nghiệp vụ tính theo `create_time` giờ Việt Nam.

## Báo cáo đồ án

**Nhận xét của GVHD (TS. Phạm Thị Nguyệt, 20/11/2026), phải giữ khi sửa tiếp**
- Bố cục sinh viên chọn: Mở đầu (5 mục) → Chương 1 Cơ sở lý thuyết và tổng quan nghiên cứu (1.1 luồng dữ liệu, 1.2 MLOps, 1.3 dự báo/tồn kho, 1.4 nghiên cứu liên quan và khoảng trống) → Chương 2 Thu thập dữ liệu và Data Lake → Chương 3 ETL, Bronze/Silver/Gold và kho dữ liệu → Chương 4 Mô hình, quản lý mô hình và báo cáo Power BI → Kết luận → **Khai báo sử dụng AI (phần riêng sau Kết luận)** → Tài liệu tham khảo → Phụ lục A–F.
- Mỗi chương nhiệm vụ có mục thực nghiệm riêng; lựa chọn công nghệ tách theo từng chương.
- **Văn phong:**
  - không tự đặt câu hỏi tu từ;
  - không rào đón lặp lại kiểu AI;
  - không viết câu sáo rỗng ("vì thời gian có hạn…").
- Bìa theo trang 2 của Phụ lục 3 mẫu khoa: khung viền, "TRƯỜNG ĐH MỎ - ĐỊA CHẤT", logo (trích từ PDF mẫu), "ĐỒ ÁN TỐT NGHIỆP", sinh viên, mã sinh viên, GVHD. Tên đề tài là phần em thêm.
- **Nguồn dữ liệu:**
  - Dữ liệu thật là dữ liệu bảo mật, nên đồ án dùng bản trích bộ đơn hàng mẫu đã ẩn danh: `~/Downloads/sho_order_anonymized_*.csv` (5.000 dòng × 84 cột) và `tts_order_anonymized_*.csv` (2.000 × 71), khoảng 25/12/2025–10/08/2026.
  - Kịch bản giả lập được xây dựng từ cấu trúc của bộ này.
  - Lý do chọn streaming: bài toán vận hành, cần can thiệp trong ngày.
- **Tài liệu tổng quan:**
  - Bốn tài liệu GVHD gửi đã kiểm tra là có thật, thông tin khớp: Kreuzberger 2023, Subramanya 2022, Khattach 2025, Makridakis M5 2022.
  - Bổ sung thêm: Theodorou 2022, Croston, SBA, TSB, Kappa (Kreps 2014), Lambda (Marz & Warren).
- **Đánh số:**
  - Bảng, hình đánh theo phần: MĐ-n, 1-n … 4-n, KL-n, AI-n, PL-n; công thức đánh (chương.n).
  - Bản mới không có trường SEQ, nên Update Field trên mục lục và danh mục là an toàn.

**Cẩm nang và nhóm đề tài**
- Đề tài thuộc **nhóm 5** của Cẩm nang ĐATN ngành KHDL (Dữ liệu lớn, kiến trúc Data Lake/Data Warehouse) và có nhánh **nhóm 4** (chuỗi thời gian).
- Đối chiếu từng yêu cầu ở Bảng KL-4 của báo cáo.
- Giải trình vì sao không dùng Spark/Hadoop: khối lượng nhỏ (khoảng 160 nghìn dòng, kho thô vài chục MB), và tính đúng dữ liệu được bảo đảm bằng khóa và giao dịch PostgreSQL.

**Cách gửi chỉnh sửa (sinh viên yêu cầu ngày 21/11/2026, áp dụng từ nay)**
- **Không tạo báo cáo mới.** Sửa trực tiếp vào tệp báo cáo hiện hành, giữ nguyên tên tệp, để sinh viên hoàn thiện một bản xuyên suốt. Từ 22/11 (15h13) sinh viên đổi tên tệp thành `output/bao-cao-do-an-2026-11-21/BaoCao_DATN_DinhCongMinh.docx` và mở bằng **WPS Office** (tệp bị khóa khi WPS đang mở; nhờ sinh viên lưu và đóng WPS rồi mới ghi, và chép lại bản mới nhất sau khi đóng).
- Ảnh Power BI trong báo cáo (Hình 3-2, 4-4 đến 4-7 = `word/media/image11, 18–21.png`) chụp lại ngày 22/11 bằng PrintWindow, chỉ phần trang báo cáo (đã thu gọn các pane), lưu ở `output/bao-cao-do-an-2026-11-22/anh-power-bi/`. Đã thay vào docx lúc 23h12 cùng 5 chỗ chữ: ngày "17/11/2026" → "22/11/2026" ở chú thích Hình 3-2, danh mục hình và câu dẫn 4.5.3; câu "Giao diện các trang sẽ tiếp tục được sinh viên hoàn thiện" → "Báo cáo sẽ tiếp tục được phát triển theo những cập nhật của pipeline và hệ thống"; 4.5.1 "63 measure" → "64 measure". Sao lưu trước khi sửa ở scratchpad phiên (`anh-pbi/sao-luu/`).
- Ngày 23/11 (00h18–00h45), theo yêu cầu sinh viên, chụp lại 7 ảnh Phụ lục 6 (Hình PL-1 đến PL-7 = `word/media/image32–38.png`) vì ảnh cũ có thanh địa chỉ bị chọn bôi xanh. Bật stack, chạy `ingestion/producer.py` khoảng 24 phút (khoảng 2.400 sự kiện mô phỏng đã vào kho, ngày 23/11), chụp khi luồng đang chạy, rồi chờ lag 0 / pending 0 và `docker compose --profile demo stop`. Câu dẫn Phụ lục 6 đổi ngày chụp thành 23/11/2026. Ảnh ở `output/bao-cao-do-an-2026-11-23/anh-phu-luc/`. Cách chụp trình duyệt cho ảnh "tự nhiên": Chrome hồ sơ tạm, mở mỗi trang bằng tab mới có sẵn URL (DevTools `Target.createTarget`) để thanh địa chỉ không bị chọn, bỏ focus ô nhập tự focus (Prometheus), bật Pretty-print cho JSON, chuột để ở góc màn hình. Log pipeline-worker (PL-4) lọc bỏ dòng `kafka.conn`/`kafka.coordinator` để thấy chu kỳ ETL, coverage và mart refresh.
- Ngày 23/11 (03h08), theo yêu cầu sinh viên, thay MC 19 (NCKH Traffic_Analysis, vì sinh viên chỉ giao việc, không đưa kiến thức) bằng lượt Claude Code 16/11 "bảng vật lý trong schema mart là gì… giữ dữ liệu theo ngày và thêm phiên bản nào dự báo cho ngày đó…" (sinh viên đưa yêu cầu thiết kế tầng Gold, AI làm "theo ba yêu cầu của bạn"). Nhóm 6 đổi tên "Vận dụng kiến thức" (đưa kiến thức của mình, kể cả từ dự án khác, để AI làm theo): MC 19 tầng Gold, MC 20 Build phase. Lịch sử trên máy không có lượt NCKH nào sinh viên đưa kiến thức partition/vi mẻ (có thể đã làm bằng công cụ khác). Báo cáo 124 trang.
- Ngày 23/11 (02h55), theo yêu cầu sinh viên, sửa lại bộ minh chứng (vẫn 20): bỏ MC "bố cục ba nhiệm vụ theo tầng dữ liệu" và MC "tách NiFi"; thêm **Nhóm 6 - Vận dụng kiến thức từ dự án khác** (sinh viên cho phép dùng hội thoại ngoài đồ án vì đã áp dụng vào đồ án; dòng đầu mỗi ảnh ghi rõ tên dự án): MC 19 dự án NCKH Traffic_Analysis (Antigravity 30/09: gom lô vi mẻ, phân vùng Parquet year/month/day, bảng PostgreSQL partitioned), MC 20 dự án Build phase (Antigravity 16/10: sinh viên tự đề ra ETL 3 lớp Bronze/Silver/Gold để truy vết khi số liệu báo cáo bị phản ánh; trích phần sinh viên tự viết, bỏ phần định nghĩa dán từ web). Nhóm: điều phối 1-4, quyết định 5-7, kiểm chứng 8-11, vấn đáp 12-15, kiểm tra chéo 16-18, vận dụng 19-20. **NotebookLM** (tìm hiểu data model) chưa có minh chứng: là công cụ web, không có lịch sử trên máy; cần sinh viên cung cấp ảnh chụp hoặc nội dung. Sinh viên sửa tay bằng WPS lúc 02h49 (giữ nguyên; WPS đổi một số ngoặc kép về dạng cong). Báo cáo 123 trang.
- Ngày 23/11 (02h20), theo yêu cầu sinh viên, thêm 2 minh chứng AI (tổng **20**, thay số ở ghi chú 01h40): MC 5 "tự đặt bố cục ba nhiệm vụ theo các tầng dữ liệu, yêu cầu nêu rõ vì sao xử lý theo luồng và vì sao chia ba tầng Bronze, Silver, Gold" (Claude Code, tin nhắn tối 20/11) và MC 14 "hỏi vì sao luồng cần các tầng đệm thay vì ghi thẳng vào cơ sở dữ liệu" (Antigravity 06/10). Nhóm mới: điều phối 1-5, quyết định 6-9, kiểm chứng 10-13, vấn đáp 14-17, kiểm tra chéo 18-20. Lưu ý: cơ sở dữ liệu Antigravity có cả hội thoại của dự án khác (`workspace_uris` như `vinai/Build phase`); hội thoại "GitHub Repo Progress Review" có câu về ETL 3 lớp Bronze nhưng **thuộc dự án khác**, không dùng làm minh chứng. Luôn kiểm tra `workspace_uris` trước khi trích. Sinh viên sửa tay bằng WPS lúc 02h16 (giữ nguyên; có lỗi gõ "năm p\hần" ở Bảng PL-4 dòng 8, chưa sửa). Báo cáo 122 trang.
- Ngày 23/11 (01h50), theo yêu cầu sinh viên: **hạn chế ghi ngày trong báo cáo** (các tài liệu trong `docs/` đã ghi rõ), để báo cáo tập trung vào nội dung chính. Đã bỏ ngày chạy thử/ngày chụp ở 38 chỗ: tên Bảng 2-4, 2-8, 3-10, 4-6, 4-7, 4-8 (và danh mục bảng), các cụm "ngày 16/11/2026, …", "lần làm mới ngày 22/11/2026", "ngày 23/11/2026" ở câu dẫn Phụ lục 6, mốc ngày trong Bảng AI-1/PL-4; ảnh minh chứng AI không còn ngày giờ. **Giữ** ngày khi là nội dung: khoảng thời gian của bộ dữ liệu mẫu và dữ liệu sinh ra, ngày đôi 10/10 và 11/11, ngày ngoại lai, bảng đối soát theo ngày nguồn, mốc thời gian dùng từng công cụ AI trong phần Khai báo, năm xuất bản tài liệu tham khảo, bìa và thời gian thực hiện. Khi viết thêm, không thêm ngày chạy thử vào tên bảng/hình hay câu văn.
- Ngày 23/11 (01h40), theo yêu cầu sinh viên, làm lại ảnh minh chứng AI ở Phụ lục 5: 18 ảnh (thay 10 ảnh cũ), chọn từ 27/09 đến 22/11 trên cả Antigravity, Codex, Claude Code, chia 5 nhóm theo vai trò sinh viên, mỗi nhóm có một đoạn dẫn: **điều phối và giao việc** (MC 1-4), **quyết định về hệ thống** (5-8), **kiểm chứng kết quả** (9-12), **vấn đáp hỏi vì sao** (13-15), **kiểm tra chéo** (16-18). Mỗi ảnh có thể gồm 2 lượt (thấy cả câu chốt quyết định), ghi công cụ và giờ từng lượt, trích nguyên văn, lược bằng [...]. Bảng PL-4 giữ nguyên. Ảnh và `captions.json` ở `output/bao-cao-do-an-2026-11-23/minh-chung-ai/`. Script trong scratchpad phiên: `aihist/ev2.py` (trích lượt hội thoại; Claude Code lấy câu trả lời cuối của lượt, tính cả tin nhắn `queue-operation`), `aihist/render_ev2.py` (danh sách 18 mục + dựng ảnh), `aihist/mc_docx.py` (thay vào docx). Bản sao lịch sử Claude Code mới: `aihist/claude-2026-11-23/`. Tài liệu còn 121 trang; số trang mục lục đã cập nhật bằng Word thật.
- Ngày 23/11 (01h20), theo yêu cầu sinh viên:
  - **Tên hình chỉ ghi tên**, không thêm ghi chú trong ngoặc (loại ảnh, ngày chụp, nguồn). Đã bỏ ở Hình 2-4, 2-5, 2-6, 2-7, 3-2, 3-5, 4-3 (cả chú thích và danh mục hình). Loại ảnh vẫn nêu trong câu dẫn khi cần (ví dụ câu dẫn Phụ lục 6, mục 4.5.3) để giữ quy tắc báo cáo trung thực.
  - **Ký hiệu gõ từ bàn phím**: `<=`, `>=`, `<>`, `->`, `-` (thay cho gạch ngang dài và dấu trừ toán học), `x`, `~`, `...`, `'`. **Ngoặc kép dùng dạng cong `“ ”`** (sinh viên đổi ý lúc 02h58 ngày 23/11; đã đổi toàn bộ 34 dấu ngoặc thẳng còn lại, mở/đóng theo ngữ cảnh). Đã đổi toàn bộ trong thân bài và chân trang. Giữ nguyên ký hiệu toán học trong công thức (Σ, σ, √, ŷ, d̄, dấu nhân `·`). Khi viết thêm nội dung, dùng đúng quy ước này. Sinh viên tự đổi chú thích Hình 3-1 sang dấu `-` lúc 01h14.
  - Sau khi sửa, tài liệu còn 116 trang; đã cập nhật số trang của 3 trường TOC (mục lục, danh mục bảng, danh mục hình) bằng Microsoft Word thật (`UpdatePageNumbers`, `SaveAs2`), đã đối chiếu nội dung chữ và ảnh không đổi. PDF cùng tên đã cập nhật.
- Ngày 22/11 (23h50), theo yêu cầu sinh viên: **chỉ in đậm tiêu đề**. Đã bỏ in đậm trực tiếp ở 68 run chữ thường (đầu gạch đầu dòng "Mục tiêu n:", "Chương n:", nhãn đầu đoạn "Biến động.", "Phạm vi dữ liệu:", chữ nhấn mạnh "at-least-once", "Bronze/Silver/Gold", dòng "Tổng 13 container" của bảng tài nguyên). Giữ in đậm: tiêu đề chương/mục, dòng tiêu đề cột của bảng, trang bìa. Khi viết thêm nội dung, không in đậm chữ trong thân bài.
- Chỉ sửa đúng những nội dung đã trao đổi với sinh viên; không tự sửa phần khác.
- Sửa thẳng trên XML của tệp docx đó bằng `xml.dom.minidom`. Không dựng lại bằng `build.py`/`build2.py`, vì sẽ ghi đè phần sinh viên đã tự sửa trong Word.
- Trước khi sửa, sao lưu bản hiện tại vào thư mục tạm. Kiểm tra mở được bằng Word; nếu mục lục hoặc số trang đổi thì cập nhật trường TOC bằng Word.
- Nếu sinh viên đang làm trên Google Docs hoặc một tệp khác, hỏi tệp nào là bản đang dùng trước khi sửa.
- (Trước 21/11 từng dùng tệp .docx có theo dõi thay đổi, tác giả "Claude (AI)"; nay không dùng nữa trừ khi sinh viên yêu cầu.)
- Tải Google Doc bằng `https://docs.google.com/document/d/<ID>/export?format=docx`; tài liệu phải ở chế độ chia sẻ "Anyone with the link".
- Không có lxml (bị chặn cài), nên sửa XML bằng `xml.dom.minidom`.
- Kiểm tra lại sau khi sửa:
  - từ chối tất cả thay đổi thì phải ra đúng bản gốc;
  - mở được bằng Microsoft Word. **Lưu ý (phát hiện 22/11):** ProgID `Word.Application` ở HKCU bị WPS chiếm (trỏ tới `wps.exe /Automation`), nên `New-Object -ComObject Word.Application` (kể cả `docx2pdf.ps1`) thực ra chạy WPS; `Quit()` hay lỗi RPC và để lại tiến trình `wps.exe /Automation -Embedding` giữ khóa tệp docx (WPS hiện tệp ở chế độ "[Read-Only]"). Muốn dùng Word thật: **không dùng `GetActiveObject("Word.Application")`** khi WPS đang mở — ngày 23/11 cách này bám nhầm vào WPS đang chạy và lệnh `Quit()` đã đóng cửa sổ WPS của sinh viên. Cách an toàn (script `scratchpad/anh-pbi/msword_export.ps1`): chép tệp sang thư mục tạm, mở bản chép bằng `WINWORD.EXE /q /n <bản chép>`, lấy Document qua `[Runtime.InteropServices.Marshal]::BindToMoniker(<đường dẫn bản chép>)`, kiểm tra `Application.Path` chứa "Microsoft Office" rồi mới thao tác, xong thì Close/Quit và dừng đúng tiến trình WINWORD đó; sau cùng dọn các `wps.exe … -Embedding` mà file moniker có thể kích hoạt. Sau khi dùng COM, kiểm tra và dừng các `wps.exe … /Automation -Embedding` còn sót. Không sửa registry;
  - xuất PDF và vẽ trang ra PNG bằng `Windows.Data.Pdf` để xem.
- Ảnh và đầu ra đặt ở `output/bao-cao-do-an-<ngày>/`.

**Minh chứng**
- (Chỉ áp dụng cho bản cũ 17/11: caption còn trường SEQ nên không dùng "Update Field".)
- **Minh bạch AI:**
  - Sinh viên dùng Antigravity (27/09–09/10), Codex (09/10–09/11) và Claude Code (từ 15/11).
  - Minh chứng ở Phụ lục E và Bảng PL-4, trích từ lịch sử cục bộ: `~/.gemini/antigravity/conversation_summaries.db`, `~/.codex/state_5.sqlite` và `thread_history_1.sqlite` (bỏ các phiên "Guardian review"), `~/.claude/projects/`.
  - Đọc bản sao, không đọc trực tiếp.

**Tài liệu và bằng chứng chính**
- `docs/04-pipeline-updates/` lần 12–15:
  - chạy lại dữ liệu và giới hạn MinIO;
  - lớp dữ liệu báo cáo;
  - data mart;
  - xung đột mã đơn.
- `docs/07-bao-cao-do-an/`:
  - `06-power-bi-va-tong-hop-thuc-nghiem.md` mục 9: PBIP và đối chiếu số;
  - `kiem-tra-e2e-va-yeu-cau-pipeline-thuc-te-2026-11-16.md`;
  - `kiem-tra-van-hanh-streaming-1-gio-2026-11-19.md`;
  - `evidence/`.
- `output/`:
  - `streaming-soak-2026-11-19/`: số đo, ảnh Chrome headless, `desktop/` là ảnh chụp màn hình thật;
  - `fault-recovery-2026-11-16/`, `replay-determinism-2026-11-16/`, `model-retrain-2026-11-16/`, `order-id-repair-20261117/`.

## Lưu ý môi trường Windows

- **Mã hóa:**
  - Thư mục dự án có dấu tiếng Việt (`Đếm ngày xa trường`). PowerShell 5.1 đọc `.ps1` không BOM theo ANSI, làm hỏng đường dẫn và từng tạo thư mục tên lỗi trong `C:\Users\MINH`. Luôn ghi `.ps1` dạng UTF-8 **có BOM**.
  - Không dùng `Get-Content`/`Set-Content` để sửa tệp UTF-8 không BOM; dùng Python, công cụ Edit hoặc `[IO.File]` với UTF8.
  - Python in tiếng Việt ra console cần `PYTHONIOENCODING=utf-8`; `python -I` bỏ qua biến này, khi đó dùng `sys.stdout.reconfigure(encoding="utf-8")`.
- **Heredoc trong Bash tool** có thể đổi `\\` thành `\` và `\n` thành ký tự xuống dòng thật. Viết script dài bằng công cụ Write rồi chạy, thay vì heredoc.
- **PowerShell gọi từ Python** cần `-ExecutionPolicy Bypass` khi nạp `.ps1`.
- **Chụp màn hình desktop thật:**
  - Gọi `SetProcessDPIAware` trước khi chụp; màn hình 1920×1200, có scaling.
  - Đưa cửa sổ lên trước bằng `ShowWindow` và `SetForegroundWindow`; dùng `conhost.exe powershell …` để có cửa sổ console riêng.
  - Task Manager chạy quyền quản trị nên không điều khiển được; trang Processes lộ ứng dụng cá nhân, không đưa vào báo cáo.
- **Docker Desktop** có thể tự bật lại container có restart policy (`nifi`, CVAT). Kiểm tra `docker ps` trước khi đo.
- **Test dài:** máy từng vào Modern Standby giữa chừng. Giữ máy thức bằng `SetThreadExecutionState(0x80000003)` trong một tiến trình PowerShell riêng, rồi tắt tiến trình đó khi xong.

## Quy ước

- Commit message dùng tiền tố conventional và kết thúc bằng ` [AI-assisted]` cho phần việc có AI hỗ trợ (yêu cầu minh bạch AI của đồ án).
- Mỗi nhật ký và báo cáo tiến độ có mục "Minh bạch sử dụng AI", tách phần AI hỗ trợ và phần sinh viên tự thực hiện.
- Báo cáo nằm trong `docs/`:
  - `04-pipeline-updates/`: nhật ký thay đổi, đánh số "Lần n", có hàng chỉ mục trong README;
  - `05-how-to-run/`;
  - `06-weekly-progress/`;
  - `07-bao-cao-do-an/`: báo cáo đồ án và evidence.
- Đầu ra sinh tự động nằm trong `output/`.
- Migration sửa dữ liệu: mặc định chạy diễn tập rồi rollback, `--apply` mới commit; giữ `pipeline_lock`; ghi bằng chứng trước và sau vào `docs/07-bao-cao-do-an/evidence/`; sau đó chạy `python -m warehouse.reporting.mart --full`.
