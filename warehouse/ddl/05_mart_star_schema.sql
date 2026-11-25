-- 05_mart_star_schema.sql — Data mart dạng star schema cho báo cáo quản lý (16/11/2026).
--
-- Tầng public là kho tích hợp do pipeline ghi; mart là bảng vật lý tính sẵn cho Power BI,
-- được làm mới bởi mart.refresh() (warehouse/reporting/mart_refresh.sql) sau mỗi chu kỳ ETL
-- và sau mỗi lượt dự báo theo lô. Chạy lại tệp này không xóa dữ liệu.
--
-- Quy ước:
--   * date_key dạng yyyymmdd (20261116). dim_date liên tục, không có dòng "không xác định"
--     (Power BI yêu cầu bảng ngày liên tục, không trống).
--   * Mọi dimension khác có dòng khóa -1 "Không xác định" (nguồn THIẾU dữ liệu); fact không chứa khóa NULL
--     (trừ các mốc thời gian chưa xảy ra của fact_sales_order).
--   * dim_carrier và dim_model có thêm dòng -2 "Không áp dụng" (thuộc tính KHÔNG có nghĩa với dòng fact đó):
--     đơn chưa từng giao thì chưa có đơn vị vận chuyển; đề xuất không dùng dự báo AI thì không có phiên bản model.
--   * Không dùng ký tự phần trăm trong tệp: psycopg2 coi đó là tham số khi chạy qua exec_driver_sql.

CREATE SCHEMA IF NOT EXISTS mart;

-- ═══════════ DIMENSIONS ═══════════

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    day_of_month        SMALLINT NOT NULL,
    iso_day_of_week     SMALLINT NOT NULL,           -- 1 = Thứ Hai ... 7 = Chủ Nhật
    day_name_vi         TEXT NOT NULL,
    iso_week            SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    month_label         TEXT NOT NULL,               -- 'Tháng 11'
    quarter             SMALLINT NOT NULL,
    year                SMALLINT NOT NULL,
    year_month          INTEGER NOT NULL,            -- 202611, dùng để sắp xếp
    year_month_label    TEXT NOT NULL,               -- '11/2026'
    is_weekend          BOOLEAN NOT NULL,
    is_mega_sale        BOOLEAN NOT NULL,            -- ngày đôi 1/1 ... 12/12
    is_data_confirmed   BOOLEAN NOT NULL DEFAULT FALSE  -- ngày đã xác nhận đủ dữ liệu (coverage warehouse_live)
);

CREATE TABLE IF NOT EXISTS mart.dim_platform (
    platform_key   SMALLINT PRIMARY KEY,
    platform_code  TEXT NOT NULL UNIQUE,
    platform_name  TEXT NOT NULL
);
INSERT INTO mart.dim_platform VALUES (-1, 'unknown', 'Không xác định'), (1, 'shopee', 'Shopee'), (2, 'tiktok', 'TikTok Shop')
ON CONFLICT (platform_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_key              INTEGER PRIMARY KEY,     -- giữ surrogate key của public.Dim_Products
    sku                      TEXT NOT NULL,
    product_name             TEXT NOT NULL,
    category                 TEXT NOT NULL,
    abc_class                TEXT NOT NULL,           -- A/B/C, 'Chưa phân nhóm'
    xyz_class                TEXT NOT NULL,
    matrix_segment           TEXT NOT NULL,
    service_level            NUMERIC(4, 3) NOT NULL,  -- A 0.98, B 0.95, C 0.90, khác 0.95
    reference_selling_price  NUMERIC(14, 2),          -- giá bán tham chiếu, không phải giá vốn
    current_lead_time_days   INTEGER,                 -- từ snapshot tồn kho mới nhất; NULL = chưa biết
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO mart.dim_product (product_key, sku, product_name, category, abc_class, xyz_class, matrix_segment, service_level)
VALUES (-1, 'Không xác định', 'Không xác định', 'Không xác định', 'Chưa phân nhóm', 'Chưa phân nhóm', 'Chưa phân nhóm', 0.95)
ON CONFLICT (product_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_shop (
    shop_key      INTEGER PRIMARY KEY,
    shop_id       TEXT NOT NULL,
    shop_name     TEXT NOT NULL,
    platform_key  SMALLINT NOT NULL REFERENCES mart.dim_platform(platform_key)
);
INSERT INTO mart.dim_shop VALUES (-1, 'UNKNOWN', 'Không xác định', -1) ON CONFLICT (shop_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_geography (
    geo_key   INTEGER PRIMARY KEY,
    region    TEXT NOT NULL,
    state     TEXT NOT NULL,
    city      TEXT NOT NULL,
    district  TEXT NOT NULL,
    country   TEXT NOT NULL
);
INSERT INTO mart.dim_geography VALUES (-1, 'Không xác định', 'Không xác định', 'Không xác định', 'Không xác định', 'VN')
ON CONFLICT (geo_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_payment (
    payment_key     INTEGER PRIMARY KEY,
    payment_method  TEXT NOT NULL,
    is_cod          BOOLEAN NOT NULL
);
INSERT INTO mart.dim_payment VALUES (-1, 'Không xác định', FALSE) ON CONFLICT (payment_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_carrier (
    carrier_key   INTEGER PRIMARY KEY,
    carrier_name  TEXT NOT NULL
);
INSERT INTO mart.dim_carrier VALUES (-1, 'Không xác định'), (-2, 'Chưa giao vận chuyển') ON CONFLICT (carrier_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_order_status (
    status_key     SMALLINT PRIMARY KEY,
    order_status   TEXT NOT NULL UNIQUE,     -- mã gốc của sàn
    status_group   TEXT NOT NULL,            -- Chờ thanh toán / Đang xử lý / Hoàn tất / Hủy / Hoàn trả
    funnel_order   SMALLINT NOT NULL,        -- thứ tự trên phễu
    is_final       BOOLEAN NOT NULL
);
INSERT INTO mart.dim_order_status VALUES
    (-1, 'Không xác định', 'Không xác định', 99, FALSE),
    (1, 'UNPAID', 'Chờ thanh toán', 1, FALSE),
    (2, 'READY_TO_SHIP', 'Đang xử lý', 2, FALSE),
    (3, 'AWAITING_SHIPMENT', 'Đang xử lý', 2, FALSE),
    (4, 'AWAITING_COLLECTION', 'Đang xử lý', 2, FALSE),
    (5, 'SHIPPED', 'Đang giao', 3, FALSE),
    (6, 'DELIVERED', 'Đã giao', 4, FALSE),
    (7, 'COMPLETED', 'Hoàn tất', 5, TRUE),
    (8, 'IN_CANCEL', 'Hủy', 6, FALSE),
    (9, 'CANCELLED', 'Hủy', 6, TRUE),
    (10, 'TO_RETURN', 'Hoàn trả', 7, FALSE)
ON CONFLICT (status_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS mart.dim_model (
    model_key             INTEGER PRIMARY KEY,
    model_name            TEXT NOT NULL,         -- thuật toán: naive, moving_average, ...
    model_version         TEXT NOT NULL,
    training_data_origin  TEXT NOT NULL,
    version_label         TEXT NOT NULL,         -- 'v4 · naive'
    model_stage           TEXT,
    cv_wape               NUMERIC(10, 3),        -- WAPE backtest (đơn vị phần trăm)
    cv_bias               NUMERIC(10, 3),        -- bias backtest, âm = dự báo thiếu
    evaluation_protocol   TEXT,
    first_forecast_at     TIMESTAMP,             -- giờ Việt Nam
    last_forecast_at      TIMESTAMP,             -- giờ Việt Nam
    is_serving            BOOLEAN NOT NULL DEFAULT FALSE,   -- phiên bản trong lượt dự báo theo lô gần nhất
    UNIQUE (model_name, model_version, training_data_origin)
);
CREATE SEQUENCE IF NOT EXISTS mart.dim_model_key_seq START 1;
INSERT INTO mart.dim_model (model_key, model_name, model_version, training_data_origin, version_label)
VALUES (-1, 'unknown', 'unknown', 'unknown', 'Không xác định'),
       (-2, 'not_applicable', 'n/a', 'not_applicable', 'Không dùng dự báo AI')
ON CONFLICT (model_key) DO NOTHING;

-- ═══════════ FACTS ═══════════

-- Giao dịch: một dòng hàng. sales_line_key = public.Fact_Orders.order_fact_id.
CREATE TABLE IF NOT EXISTS mart.fact_sales_line (
    sales_line_key   BIGINT PRIMARY KEY,
    order_id         TEXT NOT NULL,                 -- degenerate dimension
    order_date_key   INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    product_key      INTEGER NOT NULL REFERENCES mart.dim_product(product_key),
    platform_key     SMALLINT NOT NULL REFERENCES mart.dim_platform(platform_key),
    shop_key         INTEGER NOT NULL REFERENCES mart.dim_shop(shop_key),
    geo_key          INTEGER NOT NULL REFERENCES mart.dim_geography(geo_key),
    status_key       SMALLINT NOT NULL REFERENCES mart.dim_order_status(status_key),
    is_cancelled     BOOLEAN NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    net_quantity     INTEGER NOT NULL CHECK (net_quantity >= 0),   -- 0 nếu dòng bị hủy
    unit_price       NUMERIC(14, 2),
    gross_amount     NUMERIC(14, 2) NOT NULL DEFAULT 0,            -- số tiền người mua trả (nguồn)
    net_amount       NUMERIC(14, 2) NOT NULL DEFAULT 0,            -- 0 nếu dòng bị hủy
    discount_amount  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    platform_fees    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    create_time      TIMESTAMP NOT NULL,
    source_loaded_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_mart_sales_line_date ON mart.fact_sales_line(order_date_key);
CREATE INDEX IF NOT EXISTS ix_mart_sales_line_product_date ON mart.fact_sales_line(product_key, order_date_key);
CREATE INDEX IF NOT EXISTS ix_mart_sales_line_order ON mart.fact_sales_line(platform_key, order_id);

-- Accumulating snapshot: một đơn, cập nhật khi đơn đi qua các mốc.
CREATE TABLE IF NOT EXISTS mart.fact_sales_order (
    order_key            BIGINT PRIMARY KEY,         -- order_fact_id nhỏ nhất của đơn
    order_id             TEXT NOT NULL,
    platform_key         SMALLINT NOT NULL REFERENCES mart.dim_platform(platform_key),
    shop_key             INTEGER NOT NULL REFERENCES mart.dim_shop(shop_key),
    geo_key              INTEGER NOT NULL REFERENCES mart.dim_geography(geo_key),
    payment_key          INTEGER NOT NULL REFERENCES mart.dim_payment(payment_key),
    carrier_key          INTEGER NOT NULL REFERENCES mart.dim_carrier(carrier_key),
    status_key           SMALLINT NOT NULL REFERENCES mart.dim_order_status(status_key),
    order_date_key       INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    completed_date_key   INTEGER REFERENCES mart.dim_date(date_key),   -- NULL: chưa hoàn tất
    create_time          TIMESTAMP NOT NULL,
    pay_time             TIMESTAMP,
    shipped_time         TIMESTAMP,
    completed_time       TIMESTAMP,
    cancel_time          TIMESTAMP,
    order_hour           SMALLINT NOT NULL,
    hours_to_pay         NUMERIC(10, 1),
    hours_to_ship        NUMERIC(10, 1),
    hours_to_complete    NUMERIC(10, 1),
    is_cancelled         BOOLEAN NOT NULL,
    line_count           INTEGER NOT NULL,
    gross_units          INTEGER NOT NULL,
    net_units            INTEGER NOT NULL,
    gross_amount         NUMERIC(16, 2) NOT NULL,
    net_amount           NUMERIC(16, 2) NOT NULL,
    discount_amount      NUMERIC(16, 2) NOT NULL,
    platform_fees        NUMERIC(16, 2) NOT NULL,
    shipping_fee         NUMERIC(16, 2) NOT NULL,
    has_header_conflict  BOOLEAN NOT NULL,           -- các dòng khác ngày/shop/địa lý/trạng thái hủy
    UNIQUE (platform_key, order_id)
);
CREATE INDEX IF NOT EXISTS ix_mart_sales_order_date ON mart.fact_sales_order(order_date_key);

-- Periodic snapshot: tồn kho SKU × ngày (kho trung tâm).
CREATE TABLE IF NOT EXISTS mart.fact_inventory_daily (
    product_key          INTEGER NOT NULL REFERENCES mart.dim_product(product_key),
    snapshot_date_key    INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    on_hand              INTEGER NOT NULL CHECK (on_hand >= 0),
    lead_time_days       INTEGER,
    snapshot_at          TIMESTAMP,              -- giờ Việt Nam (thời điểm sự kiện tồn kho)
    snapshot_time_known  BOOLEAN NOT NULL,
    source_loaded_at     TIMESTAMP,
    PRIMARY KEY (product_key, snapshot_date_key)
);

-- Dự báo: giữ MỌI dự báo của MỌI phiên bản để so sánh phiên bản trên cùng SKU × ngày × tầm dự báo.
-- Thực tế được ghi kèm khi ngày mục tiêu đã xác nhận đủ dữ liệu.
CREATE TABLE IF NOT EXISTS mart.fact_demand_forecast (
    forecast_key          BIGINT PRIMARY KEY,         -- public.Fact_Forecast_Predictions.forecast_id
    product_key           INTEGER NOT NULL REFERENCES mart.dim_product(product_key),
    target_date_key       INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    issued_date_key       INTEGER NOT NULL REFERENCES mart.dim_date(date_key),   -- ngày cuối lịch sử đầu vào
    model_key             INTEGER NOT NULL REFERENCES mart.dim_model(model_key),
    horizon_days          SMALLINT NOT NULL CHECK (horizon_days >= 1),
    predicted_quantity    NUMERIC(14, 3) NOT NULL CHECK (predicted_quantity >= 0),
    lower_bound           NUMERIC(14, 3),
    upper_bound           NUMERIC(14, 3),
    forecast_generated_at TIMESTAMP NOT NULL,     -- giờ Việt Nam
    is_latest_for_target  BOOLEAN NOT NULL DEFAULT FALSE,   -- bản mới nhất cho SKU × ngày mục tiêu (cùng nguồn)
    actual_quantity       NUMERIC(14, 3),                   -- NULL khi chưa chấm được
    actual_complete       BOOLEAN NOT NULL DEFAULT FALSE,   -- ngày mục tiêu đã qua và đã xác nhận đủ dữ liệu
    absolute_error        NUMERIC(14, 3)
);
CREATE INDEX IF NOT EXISTS ix_mart_forecast_target ON mart.fact_demand_forecast(product_key, target_date_key);
CREATE INDEX IF NOT EXISTS ix_mart_forecast_model ON mart.fact_demand_forecast(model_key, horizon_days);

-- Periodic snapshot: đề xuất đặt hàng nhà cung cấp SKU × ngày đánh giá.
-- Dòng của hôm nay được cập nhật theo dữ liệu mới; từ ngày hôm sau dòng đó không bị ghi lại nữa.
CREATE TABLE IF NOT EXISTS mart.fact_replenishment_daily (
    product_key              INTEGER NOT NULL REFERENCES mart.dim_product(product_key),
    evaluation_date_key      INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    model_key                INTEGER NOT NULL REFERENCES mart.dim_model(model_key),   -- phiên bản tạo dự báo dùng cho đề xuất
    inventory_date_key       INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    inventory_age_days       INTEGER NOT NULL,
    on_hand                  INTEGER NOT NULL,
    lead_time_days           INTEGER NOT NULL,
    lead_time_defaulted      BOOLEAN NOT NULL,
    service_level            NUMERIC(4, 3) NOT NULL,
    sigma_daily              NUMERIC(14, 3),
    avg_daily_demand         NUMERIC(14, 3),
    demand_lead_time         NUMERIC(14, 3),
    forecast_next_7d         NUMERIC(14, 3),
    demand_source            TEXT NOT NULL,          -- ai_forecast / historical_average / no_demand_data
    safety_stock             NUMERIC(14, 1),
    reorder_point            NUMERIC(14, 1),
    order_up_to              NUMERIC(14, 1),
    days_of_cover            NUMERIC(10, 1),
    action_status            TEXT NOT NULL,          -- CRITICAL / REORDER / OK / NO_DEMAND_DATA
    urgency                  TEXT NOT NULL,
    recommended_order_qty    NUMERIC(14, 1) NOT NULL CHECK (recommended_order_qty >= 0),
    projected_stockout_date  DATE,
    order_by_date            DATE,
    order_value_at_selling_price NUMERIC(16, 2),
    reason                   TEXT NOT NULL,
    refreshed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_key, evaluation_date_key)
);

-- ═══════════ TRẠNG THÁI LÀM MỚI ═══════════

CREATE TABLE IF NOT EXISTS mart.refresh_state (
    name   TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.refresh_log (
    refresh_id   BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL,
    mode         TEXT NOT NULL,                  -- incremental / full
    stats        JSONB NOT NULL
);

-- Nhãn thống nhất cho dòng "Không xác định" (bản đầu ghi 'UNKNOWN'); chạy lại không đổi gì.
UPDATE mart.dim_product SET sku = 'Không xác định' WHERE product_key = -1 AND sku <> 'Không xác định';
UPDATE mart.dim_order_status SET order_status = 'Không xác định' WHERE status_key = -1 AND order_status <> 'Không xác định';

-- Cột thời gian mà Power BI nạp lưu dạng TIMESTAMP giờ Việt Nam (16/11/2026): connector PostgreSQL của
-- Power BI không trả TIMESTAMPTZ dưới dạng datetimezone, khiến bước đổi múi giờ trong Power Query lỗi
-- trên mọi dòng có giá trị. Khối dưới đổi kiểu cho kho đã tạo trước đó; chỉ chạy khi cột còn kiểu cũ.
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT table_name, column_name FROM information_schema.columns
             WHERE table_schema = 'mart' AND data_type = 'timestamp with time zone'
               AND (table_name, column_name) IN (('dim_model', 'first_forecast_at'), ('dim_model', 'last_forecast_at'),
                                                ('fact_inventory_daily', 'snapshot_at'),
                                                ('fact_demand_forecast', 'forecast_generated_at'))
    LOOP
        EXECUTE 'ALTER TABLE mart.' || quote_ident(r.table_name) || ' ALTER COLUMN ' || quote_ident(r.column_name)
             || ' TYPE TIMESTAMP USING ' || quote_ident(r.column_name) || ' AT TIME ZONE ''Asia/Ho_Chi_Minh''';
    END LOOP;
END $$;

COMMENT ON SCHEMA mart IS 'Star schema vật lý cho báo cáo quản lý; làm mới bởi mart.refresh(). Power BI chỉ đọc schema này.';
