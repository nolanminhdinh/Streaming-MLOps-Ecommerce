-- =============================================================
-- views_for_powerbi.sql
-- Các SQL Views phục vụ mô hình hóa dữ liệu trên Power BI Desktop (Tuần 9)
-- =============================================================

-- 1. View Tổng quan Điều hành (Executive Daily Summary)
CREATE OR REPLACE VIEW vw_powerbi_executive_summary AS
SELECT
    d.full_date                     AS order_date,
    d.year,
    d.month,
    EXTRACT(DAY FROM d.full_date)::SMALLINT AS day,
    d.day_name,
    d.is_weekend,
    d.is_mega_sale,
    s.platform,
    COUNT(f.order_fact_id)          AS total_orders,
    SUM(f.quantity)                 AS total_units_sold,
    SUM(f.buyer_total_amount)       AS total_revenue,
    SUM(f.seller_discount)          AS total_seller_discount,
    SUM(f.platform_discount)        AS total_platform_discount,
    ROUND(AVG(f.buyer_total_amount), 0) AS average_order_value
FROM Fact_Orders f
JOIN Dim_Dates d ON f.date_key = d.date_key
JOIN Dim_Shops s ON f.shop_key = s.shop_key
WHERE f.is_cancelled = FALSE
GROUP BY
    d.full_date, d.year, d.month, d.day_name, d.is_weekend, d.is_mega_sale, s.platform;


-- 2. Demand rows are separated by origin, with Vietnam-local timestamps in the warehouse.
CREATE OR REPLACE VIEW vw_powerbi_demand_rows AS
SELECT 'unverified_warehouse'::TEXT AS data_origin, ''::TEXT AS source_dataset,
       p.sku::TEXT, p.product_name::TEXT, p.category::TEXT,
       f.create_time::DATE AS order_date, f.quantity::NUMERIC AS quantity,
       f.buyer_total_amount::NUMERIC AS revenue
FROM Fact_Orders f JOIN Dim_Products p ON p.product_key = f.product_key
WHERE f.is_cancelled = FALSE
UNION ALL
SELECT h.data_origin, h.source_dataset::TEXT, h.sku::TEXT, h.product_name::TEXT,
       NULL::TEXT AS category, h.create_time::DATE, h.quantity::NUMERIC, 0::NUMERIC
FROM Fact_Historical_Order_Lines h WHERE h.is_cancelled = FALSE;

CREATE OR REPLACE VIEW vw_powerbi_abc_xyz_by_origin AS
WITH bounds AS (
    SELECT data_origin, MIN(order_date) AS first_day, MAX(order_date) AS last_day
    FROM vw_powerbi_demand_rows GROUP BY data_origin
), totals AS (
    SELECT data_origin, sku, MAX(product_name) AS product_name, MAX(category) AS category,
           SUM(revenue) AS revenue, SUM(quantity) AS total_units
    FROM vw_powerbi_demand_rows GROUP BY data_origin, sku
), weights AS (
    SELECT *, CASE WHEN SUM(revenue) OVER (PARTITION BY data_origin) > 0
                   THEN revenue ELSE total_units END AS abc_weight
    FROM totals
), cumulative AS (
    SELECT *, SUM(abc_weight) OVER (PARTITION BY data_origin) AS total_weight,
           SUM(abc_weight) OVER (PARTITION BY data_origin ORDER BY abc_weight DESC, sku
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weight
    FROM weights
), daily AS (
    SELECT data_origin, sku, order_date, SUM(quantity) AS daily_q
    FROM vw_powerbi_demand_rows GROUP BY data_origin, sku, order_date
), stats AS (
    SELECT t.data_origin, t.sku, AVG(COALESCE(d.daily_q, 0)) AS mean_demand,
           COALESCE(STDDEV_SAMP(COALESCE(d.daily_q, 0)), 0) AS std_demand
    FROM totals t JOIN bounds b ON b.data_origin = t.data_origin
    CROSS JOIN LATERAL generate_series(b.first_day, b.last_day, INTERVAL '1 day') AS days(day)
    LEFT JOIN daily d ON d.data_origin = t.data_origin AND d.sku = t.sku AND d.order_date = days.day::DATE
    GROUP BY t.data_origin, t.sku
), classified AS (
    SELECT c.*, s.mean_demand, s.std_demand,
           CASE WHEN s.mean_demand > 0 THEN s.std_demand / s.mean_demand ELSE 999 END AS cv,
           CASE WHEN COALESCE((cum_weight - abc_weight) / NULLIF(total_weight, 0), 0) < 0.80 THEN 'A'
                WHEN COALESCE((cum_weight - abc_weight) / NULLIF(total_weight, 0), 0) < 0.95 THEN 'B'
                ELSE 'C' END AS abc_class
    FROM cumulative c JOIN stats s ON s.data_origin = c.data_origin AND s.sku = c.sku
)
SELECT sku, product_name, category, revenue, total_units,
       ROUND(COALESCE(cum_weight / NULLIF(total_weight, 0), 0) * 100, 2) AS cum_share_pct,
       abc_class, ROUND(mean_demand, 2) AS avg_daily_demand,
       ROUND(std_demand, 2) AS std_daily_demand, ROUND(cv, 2) AS cv_volatility,
       CASE WHEN cv <= 0.50 THEN 'X' WHEN cv <= 1.00 THEN 'Y' ELSE 'Z' END AS xyz_class,
       abc_class || CASE WHEN cv <= 0.50 THEN 'X' WHEN cv <= 1.00 THEN 'Y' ELSE 'Z' END AS matrix_segment,
       data_origin, mean_demand AS mean_demand_exact, std_demand AS std_demand_exact
FROM classified;

-- Existing financial/inventory dashboards remain scoped to the streaming origin.
CREATE OR REPLACE VIEW vw_powerbi_abc_xyz_matrix AS
SELECT sku::VARCHAR(50), product_name::VARCHAR(500), category::VARCHAR(100),
       revenue, total_units::BIGINT, cum_share_pct, abc_class,
       avg_daily_demand, std_daily_demand, cv_volatility, xyz_class, matrix_segment
FROM vw_powerbi_abc_xyz_by_origin WHERE data_origin = 'unverified_warehouse';

-- 3. View Giám sát Tồn kho & Cảnh báo Nhập hàng (Inventory Health & ROP Alerts)
CREATE OR REPLACE VIEW vw_powerbi_inventory_health AS
WITH latest_inv AS (
    SELECT
        p.product_key,
        i.date_key,
        p.sku,
        p.product_name,
        p.category,
        i.stock_on_hand AS current_stock,
        d.full_date     AS inventory_date,
        ROW_NUMBER() OVER (PARTITION BY p.sku ORDER BY d.full_date DESC) as rn
    FROM Fact_Inventory_Daily i
    JOIN Dim_Products p ON i.product_key = p.product_key
    JOIN Dim_Dates d ON i.date_key = d.date_key
),
matrix_params AS (
    SELECT
        sku,
        mean_demand_exact AS avg_daily_demand,
        std_demand_exact AS std_daily_demand,
        matrix_segment,
        -- Safety Stock = ceil(1.65 * std * sqrt(3)), khớp Python service level 0.95.
        CEIL(1.65 * std_demand_exact * SQRT(3)) AS safety_stock,
        -- ROP = ceil(avg * 3 + SS)
        CEIL((mean_demand_exact * 3) + CEIL(1.65 * std_demand_exact * SQRT(3))) AS reorder_point
    FROM vw_powerbi_abc_xyz_by_origin WHERE data_origin = 'unverified_warehouse'
)
SELECT
    inv.sku,
    inv.product_name,
    inv.category,
    m.matrix_segment,
    inv.current_stock,
    m.avg_daily_demand,
    m.safety_stock,
    m.reorder_point,
    CASE
        WHEN inv.current_stock <= m.safety_stock THEN 'CRITICAL'
        WHEN inv.current_stock <= m.reorder_point THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_level,
    CASE
        WHEN inv.current_stock <= m.reorder_point THEN TRUE
        ELSE FALSE
    END AS needs_reorder,
    ROUND(inv.current_stock / NULLIF(m.avg_daily_demand, 0), 1) AS days_until_stockout,
    GREATEST(0, (m.reorder_point + CEIL(m.avg_daily_demand * 7)) - inv.current_stock) AS recommended_reorder_qty,
    inv.inventory_date AS last_audited_date,
    inv.date_key,
    inv.product_key
FROM latest_inv inv
JOIN matrix_params m ON inv.sku = m.sku
WHERE inv.rn = 1;


-- 4. View Phân bổ Doanh thu Địa lý Toàn quốc (Geographic Sales Distribution)
CREATE OR REPLACE VIEW vw_powerbi_geographic_sales AS
SELECT
    g.state,
    g.region,
    s.platform,
    COUNT(f.order_fact_id)    AS order_count,
    SUM(f.quantity)           AS total_units,
    SUM(f.buyer_total_amount) AS total_revenue
FROM Fact_Orders f
JOIN Dim_Geography g ON f.geo_key = g.geo_key
JOIN Dim_Shops s ON f.shop_key = s.shop_key
WHERE f.is_cancelled = FALSE
GROUP BY g.state, g.region, s.platform;


-- 5. Accuracy requires matching provenance and explicitly confirmed daily coverage.
CREATE OR REPLACE VIEW vw_powerbi_forecast_vs_actual AS
WITH ranked_forecasts AS (
    SELECT fp.*, ROW_NUMBER() OVER (
        PARTITION BY product_key, target_date, data_origin, source_datasets
        ORDER BY forecast_generated_at DESC, forecast_id DESC
    ) AS forecast_rank
    FROM Fact_Forecast_Predictions fp
    WHERE model_source IN ('mlflow_registry', 'local_joblib') AND history_source = 'warehouse'
      AND target_date > (forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE
), evaluated AS (
    SELECT rf.*, p.sku, p.product_name, p.category, d.date_key,
           CASE WHEN rf.target_date < (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE AND
               CASE
                   WHEN rf.data_origin IN ('unverified_warehouse', 'warehouse_live') THEN EXISTS (
                       SELECT 1 FROM Warehouse_Daily_Coverage c
                       WHERE c.data_origin = rf.data_origin AND c.source_dataset = '' AND c.full_date = rf.target_date
                   )
                   WHEN rf.data_origin = 'public_anonymized_historical_csv' THEN
                       jsonb_array_length(rf.source_datasets) > 0 AND NOT EXISTS (
                           SELECT 1 FROM jsonb_array_elements_text(rf.source_datasets) AS ds(dataset)
                           WHERE NOT EXISTS (
                               SELECT 1 FROM Warehouse_Daily_Coverage c
                               WHERE c.data_origin = rf.data_origin AND c.source_dataset = ds.dataset AND c.full_date = rf.target_date
                           )
                       )
                   ELSE FALSE
               END THEN TRUE ELSE FALSE END AS actual_complete,
           CASE WHEN rf.data_origin IN ('unverified_warehouse', 'warehouse_live') THEN (
                    SELECT SUM(f.quantity)::NUMERIC FROM Fact_Orders f
                    WHERE f.product_key = rf.product_key AND f.create_time::DATE = rf.target_date AND f.is_cancelled = FALSE
                ) WHEN rf.data_origin = 'public_anonymized_historical_csv' THEN (
                    SELECT SUM(h.quantity)::NUMERIC FROM Fact_Historical_Order_Lines h
                    WHERE h.sku = p.sku AND h.create_time::DATE = rf.target_date AND h.is_cancelled = FALSE
                      AND h.data_origin = rf.data_origin AND rf.source_datasets ? h.source_dataset::TEXT
                ) ELSE NULL::NUMERIC END AS observed_demand
    FROM ranked_forecasts rf
    JOIN Dim_Products p ON p.product_key = rf.product_key
    JOIN Dim_Dates d ON d.full_date = rf.target_date
    WHERE rf.forecast_rank = 1
)
SELECT e.date_key, e.product_key, e.target_date AS date, e.sku, e.product_name, e.category,
       COALESCE(m.matrix_segment, 'NA') AS matrix_segment,
       CASE WHEN actual_complete THEN COALESCE(observed_demand, 0) ELSE NULL::NUMERIC END AS actual_demand,
       predicted_quantity AS forecast_demand, NULL::NUMERIC AS lower_bound, NULL::NUMERIC AS upper_bound,
       CASE WHEN actual_complete THEN ABS(COALESCE(observed_demand, 0) - predicted_quantity) ELSE NULL::NUMERIC END AS absolute_error,
       CASE WHEN actual_complete THEN POWER(COALESCE(observed_demand, 0) - predicted_quantity, 2) ELSE NULL::NUMERIC END AS squared_error,
       model_name, model_version, model_source, forecast_generated_at,
       e.data_origin, e.source_datasets, actual_complete
FROM evaluated e LEFT JOIN vw_powerbi_abc_xyz_by_origin m ON m.sku = e.sku
    AND m.data_origin = CASE WHEN e.data_origin = 'warehouse_live' THEN 'unverified_warehouse' ELSE e.data_origin END;
