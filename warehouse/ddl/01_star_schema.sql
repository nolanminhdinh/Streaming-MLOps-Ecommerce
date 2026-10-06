-- 01_star_schema.sql
-- Mô hình dữ liệu Chấm sao (Star Schema) cho kho dữ liệu phân tích demand.
-- Cập nhật Tuần 3: mở rộng theo schema vietnam_ecommerce (Shopee + TikTok).
--
-- Nguyên tắc thiết kế:
--   - Fact_Orders chứa tất cả đơn hàng từ mọi sàn (platform-agnostic).
--   - Dimension tables chuẩn hóa dữ liệu chung: sản phẩm, địa lý, thời gian, shop, sàn.
--   - Revenue/profit tính theo buyer_total_amount (số tiền thực tế khách trả).

-- ========== DIMENSION TABLES ==========

-- Dim_Products: chuẩn hóa SKU từ cả Shopee (item_sku) và TikTok (seller_sku)
CREATE TABLE IF NOT EXISTS Dim_Products (
    product_key     SERIAL PRIMARY KEY,
    sku             VARCHAR(50) UNIQUE NOT NULL,
    product_name    VARCHAR(500),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    unit_cost       NUMERIC(14, 2),         -- giá gốc (original_price)
    weight_kg       NUMERIC(8, 3),          -- trọng lượng sản phẩm
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Dim_Shops: thông tin shop/seller trên sàn
CREATE TABLE IF NOT EXISTS Dim_Shops (
    shop_key        SERIAL PRIMARY KEY,
    shop_id         VARCHAR(50),             -- shop_id (Shopee) hoặc shop_name (TikTok)
    shop_name       VARCHAR(255) NOT NULL,
    platform        VARCHAR(20) NOT NULL,    -- 'shopee' | 'tiktok'
    connection_id   VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(shop_id, platform)
);

-- Dim_Geography: chuẩn hóa địa chỉ giao hàng theo tỉnh/thành
CREATE TABLE IF NOT EXISTS Dim_Geography (
    geo_key         SERIAL PRIMARY KEY,
    state           VARCHAR(100) NOT NULL,   -- Tỉnh/TP
    city            VARCHAR(100),
    district        VARCHAR(100),
    country         VARCHAR(10) DEFAULT 'VN',
    region          VARCHAR(50),             -- Bắc/Trung/Nam
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(state, city, district)
);

-- Dim_Dates: lịch ngày (prefill 3 năm)
CREATE TABLE IF NOT EXISTS Dim_Dates (
    date_key        SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    day_of_week     SMALLINT,               -- 0=Mon, 6=Sun
    day_name        VARCHAR(20),
    week_of_year    SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE,
    is_mega_sale    BOOLEAN DEFAULT FALSE,   -- ngày đôi: 1/1, 2/2, ..., 12/12
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Dim_Payment: phương thức thanh toán
CREATE TABLE IF NOT EXISTS Dim_Payment (
    payment_key     SERIAL PRIMARY KEY,
    payment_method  VARCHAR(50) UNIQUE NOT NULL,  -- COD, VNPay, MoMo, ...
    is_cod          BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Dim_Carriers: đơn vị vận chuyển
CREATE TABLE IF NOT EXISTS Dim_Carriers (
    carrier_key     SERIAL PRIMARY KEY,
    carrier_name    VARCHAR(100) UNIQUE NOT NULL,  -- SPX Express, J&T, ...
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ========== FACT TABLES ==========

-- Fact_Orders: sự kiện đơn hàng từ tất cả sàn
CREATE TABLE IF NOT EXISTS Fact_Orders (
    order_fact_id       BIGSERIAL PRIMARY KEY,

    -- Khóa ngoại tới dimensions
    product_key         INTEGER REFERENCES Dim_Products(product_key),
    shop_key            INTEGER REFERENCES Dim_Shops(shop_key),
    geo_key             INTEGER REFERENCES Dim_Geography(geo_key),
    date_key            INTEGER REFERENCES Dim_Dates(date_key),
    payment_key         INTEGER REFERENCES Dim_Payment(payment_key),
    carrier_key         INTEGER REFERENCES Dim_Carriers(carrier_key),

    -- Định danh đơn hàng gốc
    order_id            VARCHAR(50) NOT NULL,      -- order_sn (Shopee) | order_id (TikTok)
    platform            VARCHAR(20) NOT NULL,      -- 'shopee' | 'tiktok'

    -- Trạng thái
    order_status        VARCHAR(30) NOT NULL,
    is_cancelled        BOOLEAN DEFAULT FALSE,
    cancel_reason       VARCHAR(255),

    -- Measures (tiền VND)
    quantity            INTEGER NOT NULL,
    original_price      NUMERIC(14, 2),            -- giá gốc 1 sản phẩm
    discounted_price    NUMERIC(14, 2),            -- giá sau giảm (nếu có)
    subtotal            NUMERIC(14, 2),            -- quantity × original_price
    buyer_total_amount  NUMERIC(14, 2),            -- số tiền thực tế khách trả
    seller_discount     NUMERIC(14, 2) DEFAULT 0,
    platform_discount   NUMERIC(14, 2) DEFAULT 0,  -- shopee_discount | platform_discount
    voucher_total       NUMERIC(14, 2) DEFAULT 0,  -- tổng voucher

    -- Phí sàn (chỉ áp dụng Shopee)
    commission_fee      NUMERIC(14, 2) DEFAULT 0,
    service_fee         NUMERIC(14, 2) DEFAULT 0,
    transaction_fee     NUMERIC(14, 2) DEFAULT 0,

    -- Phí vận chuyển
    shipping_fee        NUMERIC(14, 2) DEFAULT 0,
    original_shipping   NUMERIC(14, 2) DEFAULT 0,

    -- Thuế (chỉ áp dụng TikTok)
    tax_amount          NUMERIC(14, 2) DEFAULT 0,

    -- Timestamps
    create_time         TIMESTAMP NOT NULL,
    pay_time            TIMESTAMP,
    shipped_time        TIMESTAMP,
    completed_time      TIMESTAMP,
    cancel_time         TIMESTAMP,

    -- Metadata
    loaded_at           TIMESTAMP DEFAULT NOW(),

    -- Ràng buộc chống trùng lặp đơn hàng khi ETL chạy nhiều lần (Idempotency)
    CONSTRAINT uq_fact_orders_order_platform UNIQUE (order_id, platform)
);

-- Fact_Inventory_Daily: ảnh chụp tồn kho hàng ngày, nạp từ topic Kafka `inventory.logs`
-- (ingestion/consumer_inventory_to_postgres.py)
CREATE TABLE IF NOT EXISTS Fact_Inventory_Daily (
    inventory_fact_id  BIGSERIAL PRIMARY KEY,
    product_key        INTEGER REFERENCES Dim_Products(product_key),
    shop_key           INTEGER REFERENCES Dim_Shops(shop_key),
    date_key           INTEGER REFERENCES Dim_Dates(date_key),
    stock_on_hand      INTEGER NOT NULL DEFAULT 0,
    safety_stock       INTEGER,
    reorder_point      INTEGER,
    loaded_at          TIMESTAMP DEFAULT NOW()
);


-- ========== INDEXES ==========

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON Fact_Orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON Fact_Orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_shop ON Fact_Orders(shop_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_platform ON Fact_Orders(platform);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON Fact_Orders(order_status);
CREATE INDEX IF NOT EXISTS idx_fact_orders_create ON Fact_Orders(create_time);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_date ON Fact_Inventory_Daily(date_key);
-- Mỗi SKU chỉ có 1 ảnh chụp tồn kho / ngày (kho trung tâm) → consumer inventory.logs
-- upsert theo khóa này, bản ghi mới nhất trong ngày ghi đè bản ghi cũ.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_inventory_product_date
    ON Fact_Inventory_Daily(product_key, date_key);


-- ========== SEED: Dim_Dates (3 năm: 2025-2027) ==========

INSERT INTO Dim_Dates (full_date, day_of_week, day_name, week_of_year,
                       month, month_name, quarter, year, is_weekend, is_mega_sale)
SELECT
    d::DATE AS full_date,
    EXTRACT(DOW FROM d)::SMALLINT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(WEEK FROM d)::SMALLINT AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend,
    -- Mega-sale ngày đôi: ngày = tháng
    (EXTRACT(DAY FROM d) = EXTRACT(MONTH FROM d)) AS is_mega_sale
FROM generate_series('2025-01-01'::DATE, '2027-12-31'::DATE, '1 day') AS d
ON CONFLICT (full_date) DO NOTHING;


-- ========== SEED: Dim_Payment (phương thức thanh toán phổ biến VN) ==========

INSERT INTO Dim_Payment (payment_method, is_cod) VALUES
    ('COD', TRUE),
    ('Shopee Wallet', FALSE),
    ('VNPay', FALSE),
    ('Credit Card', FALSE),
    ('Debit Card', FALSE),
    ('SPayLater', FALSE),
    ('Bank Transfer', FALSE),
    ('MoMo', FALSE),
    ('ZaloPay', FALSE)
ON CONFLICT (payment_method) DO NOTHING;
