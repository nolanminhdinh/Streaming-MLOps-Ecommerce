-- 01_star_schema.sql
-- Mô hình dữ liệu Chấm sao (Star Schema) cho kho dữ liệu phân tích tồn kho.
-- Trạng thái: skeleton — sẽ tinh chỉnh kiểu dữ liệu, index, ràng buộc ở Tuần 3.

-- ========== DIMENSION TABLES ==========

CREATE TABLE IF NOT EXISTS Dim_Products (
    product_key     SERIAL PRIMARY KEY,
    sku             VARCHAR(50) UNIQUE NOT NULL,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    unit_cost       NUMERIC(12, 2),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS Dim_Warehouses (
    warehouse_key   SERIAL PRIMARY KEY,
    warehouse_id    VARCHAR(50) UNIQUE NOT NULL,
    warehouse_name  VARCHAR(255),
    region          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS Dim_Dates (
    date_key        SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    week_of_year    SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- ========== FACT TABLES ==========

CREATE TABLE IF NOT EXISTS Fact_Orders (
    order_fact_id   BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(50) NOT NULL,
    product_key     INTEGER REFERENCES Dim_Products(product_key),
    warehouse_key   INTEGER REFERENCES Dim_Warehouses(warehouse_key),
    date_key        INTEGER REFERENCES Dim_Dates(date_key),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(12, 2),
    revenue         NUMERIC(14, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    order_timestamp TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS Fact_Inventory_Daily (
    inventory_fact_id  BIGSERIAL PRIMARY KEY,
    product_key        INTEGER REFERENCES Dim_Products(product_key),
    warehouse_key      INTEGER REFERENCES Dim_Warehouses(warehouse_key),
    date_key           INTEGER REFERENCES Dim_Dates(date_key),
    stock_on_hand      INTEGER NOT NULL,
    safety_stock       INTEGER,
    reorder_point      INTEGER,
    loaded_at          TIMESTAMP DEFAULT NOW()
);

-- ========== INDEXES (bổ sung dần theo nhu cầu truy vấn thực tế) ==========

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON Fact_Orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON Fact_Orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_date ON Fact_Inventory_Daily(date_key);
