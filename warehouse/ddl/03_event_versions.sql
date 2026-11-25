-- Additive migration for existing installations. No facts are rewritten.
ALTER TABLE Fact_Orders ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMP;
ALTER TABLE Fact_Inventory_Daily ADD COLUMN IF NOT EXISTS snapshot_at TIMESTAMPTZ;
-- Đầu vào nghiệp vụ đặt hàng (16/11/2026): bản tin tồn kho đã có, trước đây bị bỏ qua.
-- NULL ở snapshot cũ nghĩa là chưa biết; không suy ra 0.
ALTER TABLE Fact_Inventory_Daily ADD COLUMN IF NOT EXISTS lead_time_days INTEGER;
ALTER TABLE Fact_Inventory_Daily ADD COLUMN IF NOT EXISTS incoming_stock INTEGER;
ALTER TABLE Fact_Inventory_Daily ADD COLUMN IF NOT EXISTS restock_eta TIMESTAMPTZ;
-- Khoảng tin cậy của dự báo đã phát (±1.96σ do serving tính); NULL ở dự báo cũ.
ALTER TABLE Fact_Forecast_Predictions ADD COLUMN IF NOT EXISTS lower_bound NUMERIC(14, 3);
ALTER TABLE Fact_Forecast_Predictions ADD COLUMN IF NOT EXISTS upper_bound NUMERIC(14, 3);
-- Một dòng mỗi lượt dự báo theo lô: model đang phục vụ, độ chính xác backtest (MLflow) và độ phủ SKU.
CREATE TABLE IF NOT EXISTS Model_Serving_Snapshot (
    snapshot_id          BIGSERIAL PRIMARY KEY,
    snapshot_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_registry_name  TEXT NOT NULL,
    model_version        TEXT NOT NULL,
    model_stage          TEXT,
    algorithm            TEXT,
    training_data_origin TEXT,
    run_id               TEXT,
    evaluation_protocol  TEXT,
    cv_wape              NUMERIC(10, 3),
    cv_bias              NUMERIC(10, 3),
    cv_mae               NUMERIC(14, 3),
    cv_rmse              NUMERIC(14, 3),
    cv_scored_rows       INTEGER,
    history_end          DATE,
    forecast_from        DATE,
    forecast_to          DATE,
    skus_requested       INTEGER,
    skus_model           INTEGER,
    skus_heuristic       INTEGER,
    skus_failed          INTEGER
);
