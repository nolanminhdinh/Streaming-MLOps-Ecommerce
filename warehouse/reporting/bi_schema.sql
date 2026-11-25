-- Stable BI contract. All views are schema-qualified and remain driven by live ETL facts.
-- Historical external catalogs are deliberately separated from the live merchant catalog.
CREATE SCHEMA IF NOT EXISTS bi;

CREATE OR REPLACE VIEW bi.dim_date AS
SELECT date_key, full_date, day_of_week, day_name, week_of_year, month, month_name,
       quarter, year, is_weekend, is_holiday, is_mega_sale
FROM public.Dim_Dates;

-- Phân nhóm ABC/XYZ trên toàn bộ đơn của kho (thêm cột cuối 16/11/2026 để giữ tương thích view cũ).
CREATE OR REPLACE VIEW bi.dim_product AS
SELECT p.product_key, p.sku, p.product_name, p.category, p.sub_category,
       p.unit_cost AS reference_selling_price, p.weight_kg,
       s.abc_class, s.xyz_class, s.matrix_segment
FROM public.Dim_Products p
LEFT JOIN public.vw_powerbi_abc_xyz_by_origin s ON s.sku = p.sku AND s.data_origin = 'unverified_warehouse';

CREATE OR REPLACE VIEW bi.dim_shop AS
SELECT shop_key, shop_id, shop_name, platform FROM public.Dim_Shops;

CREATE OR REPLACE VIEW bi.dim_geography AS
SELECT geo_key, state, city, district, country, region FROM public.Dim_Geography;

CREATE OR REPLACE VIEW bi.dim_platform AS
SELECT platform FROM (VALUES ('shopee'::TEXT), ('tiktok'::TEXT)) platforms(platform);

-- Một tên shop trên một sàn phải ứng với một shop_key. Nguồn sinh shop_id mới cho mỗi đơn
-- (simulator cũ 25/09/2026) làm view này có dòng; rỗng là trạng thái mong đợi.
CREATE OR REPLACE VIEW bi.quality_shop_identity AS
SELECT platform, shop_name, COUNT(*) AS shop_key_count,
       MIN(shop_key) AS canonical_shop_key, MIN(shop_id) AS example_shop_id
FROM public.Dim_Shops
GROUP BY platform, shop_name
HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW bi.quality_order_header_conflicts AS
SELECT jsonb_build_array(platform, order_id)::TEXT AS order_business_key,
       platform, order_id, COUNT(*) AS line_count,
       COUNT(DISTINCT date_key) AS date_count,
       COUNT(DISTINCT COALESCE(shop_key, 0)) AS shop_count,
       COUNT(DISTINCT COALESCE(geo_key, 0)) AS geography_count,
       COUNT(DISTINCT is_cancelled) AS cancellation_state_count
FROM public.Fact_Orders
GROUP BY platform, order_id
HAVING COUNT(DISTINCT date_key)>1 OR COUNT(DISTINCT COALESCE(shop_key,0))>1
    OR COUNT(DISTINCT COALESCE(geo_key,0))>1 OR COUNT(DISTINCT is_cancelled)>1;

CREATE OR REPLACE VIEW bi.fact_order_line AS
SELECT f.order_fact_id AS order_line_key,
       jsonb_build_array(f.platform, f.order_id)::TEXT AS order_business_key,
       f.order_id, f.source_line_id, f.platform,
       f.product_key, f.shop_key, f.geo_key, f.date_key, f.payment_key, f.carrier_key,
       f.order_status, f.is_cancelled, f.quantity,
       f.original_price AS unit_selling_price, f.subtotal AS gross_line_amount,
       f.buyer_total_amount AS buyer_amount,
       f.seller_discount, f.platform_discount, f.voucher_total,
       f.shipping_fee, f.tax_amount, f.commission_fee, f.service_fee, f.transaction_fee,
       f.create_time, f.source_updated_at, f.loaded_at,
       (f.shop_key IS NULL) AS missing_shop, (f.geo_key IS NULL) AS missing_geography,
       (q.order_id IS NOT NULL) AS header_conflict
FROM public.Fact_Orders f
LEFT JOIN bi.quality_order_header_conflicts q ON q.platform=f.platform AND q.order_id=f.order_id;

CREATE OR REPLACE VIEW bi.fact_inventory_daily AS
SELECT inventory_fact_id AS inventory_snapshot_key, product_key, date_key,
       stock_on_hand AS current_stock, snapshot_at,
       (snapshot_at IS NOT NULL) AS snapshot_time_known,
       loaded_at, 'central'::TEXT AS stock_scope
FROM public.Fact_Inventory_Daily;

-- One newest available business-date snapshot per product, not sum of daily stocks.
CREATE OR REPLACE VIEW bi.inventory_latest AS
SELECT inventory_snapshot_key, product_key, date_key, current_stock, snapshot_at,
       snapshot_time_known, loaded_at, stock_scope
FROM (
    SELECT v.*, ROW_NUMBER() OVER (PARTITION BY product_key
        ORDER BY d.full_date DESC, snapshot_at DESC NULLS LAST, inventory_snapshot_key DESC) AS rn
    FROM bi.fact_inventory_daily v JOIN public.Dim_Dates d USING(date_key)
) snapshots WHERE rn=1;

CREATE OR REPLACE VIEW bi.inventory_health AS
SELECT latest.inventory_snapshot_key, latest.product_key, latest.date_key,
       p.sku, latest.current_stock, latest.snapshot_at, latest.snapshot_time_known,
       d.full_date AS inventory_date,
       h.avg_daily_demand, h.safety_stock, h.reorder_point, h.matrix_segment,
       h.alert_level, h.needs_reorder, h.days_until_stockout, h.recommended_reorder_qty,
       (h.product_key IS NOT NULL) AS demand_policy_available
FROM bi.inventory_latest latest
JOIN public.Dim_Products p USING(product_key)
JOIN public.Dim_Dates d USING(date_key)
LEFT JOIN public.vw_powerbi_inventory_health h USING(product_key);

-- Every saved issue, including legacy/unevaluated forecasts, remains inspectable.
CREATE OR REPLACE VIEW bi.fact_forecast_issue AS
SELECT forecast_id AS forecast_issue_key, product_key, target_date,
       predicted_quantity, model_name, model_version, model_source,
       history_source, history_end_date, forecast_generated_at, data_origin, source_datasets
FROM public.Fact_Forecast_Predictions;

-- Evaluated rows retain all provenance fields. Do not sum across overlapping sources.
CREATE OR REPLACE VIEW bi.forecast_evaluation AS
SELECT date_key, product_key, date AS target_date, actual_demand, forecast_demand,
       absolute_error, squared_error, actual_complete,
       model_name, model_version, model_source, forecast_generated_at,
       data_origin, source_datasets
FROM public.vw_powerbi_forecast_vs_actual;

CREATE OR REPLACE VIEW bi.dim_historical_product AS
SELECT DISTINCT ON (data_origin, source_dataset, sku)
       jsonb_build_array(data_origin, source_dataset, sku)::TEXT AS historical_product_key,
       data_origin, source_dataset, sku, product_name
FROM public.Fact_Historical_Order_Lines
ORDER BY data_origin, source_dataset, sku, loaded_at DESC, historical_line_id DESC;

CREATE OR REPLACE VIEW bi.fact_historical_order_line AS
SELECT historical_line_id AS historical_line_key,
       jsonb_build_array(data_origin, source_dataset, sku)::TEXT AS historical_product_key,
       data_origin, source_dataset, sku, source_record_id, source_order_id, platform,
       order_status, is_cancelled, quantity, create_time, loaded_at
FROM public.Fact_Historical_Order_Lines;

-- ═══════════ Lớp quyết định cho báo cáo quản lý (16/11/2026) ═══════════

-- Dự báo mới nhất cho mỗi SKU × ngày mục tiêu từ model đang phục vụ trên kho streaming.
CREATE OR REPLACE VIEW bi.forecast_daily AS
SELECT f.forecast_id AS forecast_key, f.product_key, d.date_key, f.target_date,
       f.predicted_quantity, f.lower_bound, f.upper_bound,
       f.model_name, f.model_version, f.model_source, f.history_end_date,
       f.forecast_generated_at, (f.target_date - f.history_end_date) AS horizon_days, f.data_origin
FROM (
    SELECT fp.*, ROW_NUMBER() OVER (PARTITION BY fp.product_key, fp.target_date
                                    ORDER BY fp.forecast_generated_at DESC, fp.forecast_id DESC) AS rn
    FROM public.Fact_Forecast_Predictions fp
    WHERE fp.data_origin = 'warehouse_live'
) f
JOIN public.Dim_Dates d ON d.full_date = f.target_date
WHERE f.rn = 1;

-- Trạng thái model mới nhất (một dòng) và lịch sử các lượt dự báo theo lô.
CREATE OR REPLACE VIEW bi.model_serving_history AS
SELECT snapshot_id, snapshot_at AT TIME ZONE 'Asia/Ho_Chi_Minh' AS snapshot_at_vn, model_registry_name, model_version,
       model_stage, algorithm, training_data_origin, evaluation_protocol, cv_wape, cv_bias, cv_mae, cv_rmse,
       cv_scored_rows, history_end, forecast_from, forecast_to, skus_requested, skus_model, skus_heuristic, skus_failed
FROM public.Model_Serving_Snapshot;

CREATE OR REPLACE VIEW bi.model_status AS
SELECT * FROM bi.model_serving_history ORDER BY snapshot_id DESC LIMIT 1;

-- Đơn hàng bán: một dòng mỗi (sàn, mã đơn). Đơn bị hủy toàn bộ khi mọi dòng hủy.
-- Thuộc tính header lấy theo dòng đầu tiên; đơn có xung đột header được đánh dấu.
CREATE OR REPLACE VIEW bi.fact_sales_order AS
SELECT jsonb_build_array(f.platform, f.order_id)::TEXT AS order_business_key,
       f.platform, f.order_id,
       (ARRAY_AGG(f.shop_key ORDER BY f.order_fact_id))[1] AS shop_key,
       (ARRAY_AGG(f.geo_key ORDER BY f.order_fact_id))[1] AS geo_key,
       MIN(f.date_key) AS date_key,
       (ARRAY_AGG(f.payment_key ORDER BY f.order_fact_id))[1] AS payment_key,
       (ARRAY_AGG(f.carrier_key ORDER BY f.order_fact_id))[1] AS carrier_key,
       MIN(f.create_time) AS create_time,
       EXTRACT(HOUR FROM MIN(f.create_time))::INT AS order_hour,
       MAX(f.completed_time) AS completed_time,
       ROUND(EXTRACT(EPOCH FROM (MAX(f.completed_time) - MIN(f.create_time))) / 3600.0, 1) AS hours_to_complete,
       BOOL_AND(f.is_cancelled) AS is_cancelled,
       CASE WHEN BOOL_AND(f.is_cancelled) THEN 'CANCELLED'
            ELSE (ARRAY_AGG(f.order_status ORDER BY f.is_cancelled, f.order_fact_id))[1] END AS order_status,
       COUNT(*) AS line_count,
       SUM(f.quantity) AS gross_units,
       COALESCE(SUM(f.quantity) FILTER (WHERE NOT f.is_cancelled), 0) AS net_units,
       SUM(f.buyer_total_amount) AS gross_amount,
       COALESCE(SUM(f.buyer_total_amount) FILTER (WHERE NOT f.is_cancelled), 0) AS net_amount,
       SUM(f.seller_discount + f.platform_discount + f.voucher_total) AS total_discount,
       SUM(f.commission_fee + f.service_fee + f.transaction_fee) AS platform_fees,
       SUM(f.shipping_fee) AS shipping_fee,
       BOOL_OR(q.order_id IS NOT NULL) AS header_conflict
FROM public.Fact_Orders f
LEFT JOIN bi.quality_order_header_conflicts q ON q.platform = f.platform AND q.order_id = f.order_id
GROUP BY f.platform, f.order_id;

-- Đề xuất đặt hàng nhà cung cấp: nguồn sự thật duy nhất cho trang đặt hàng.
-- Vị thế tồn = tồn hiện có. Hàng đang về (incoming_stock/restock_eta đã được thu vào Fact_Inventory_Daily)
-- chưa đưa vào công thức theo quyết định của sinh viên 16/11/2026 — là hướng mở rộng.
-- Nhu cầu trong lead time lấy từ dự báo AI khi đủ ngày,
-- nếu không dùng trung bình lịch sử. Mức phục vụ theo nhóm ABC: A 98 phần trăm (z=2,054), B 95 (z=1,645),
-- C 90 (z=1,282); chưa phân nhóm dùng 95. SS = z·σ·√L; ROP = nhu cầu lead time + SS;
-- (không dùng ký tự phần trăm trong tệp này: psycopg2 coi nó là tham số khi chạy qua exec_driver_sql)
-- mức đặt tới = ROP + 7 ngày nhu cầu; đề xuất khi vị thế tồn ≤ ROP.
-- DROP vì danh sách cột đã đổi (bỏ cột hàng đang về); CASCADE gỡ bi.data_freshness, được tạo lại ngay bên dưới.
DROP VIEW IF EXISTS bi.replenishment_recommendation CASCADE;
CREATE VIEW bi.replenishment_recommendation AS
WITH today AS (SELECT (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE AS d),
latest AS (
    SELECT * FROM (
        SELECT i.*, dd.full_date,
               ROW_NUMBER() OVER (PARTITION BY i.product_key
                   ORDER BY dd.full_date DESC, i.snapshot_at DESC NULLS LAST, i.inventory_fact_id DESC) AS rn
        FROM public.Fact_Inventory_Daily i JOIN public.Dim_Dates dd USING (date_key)
    ) x WHERE rn = 1
),
seg AS (
    SELECT sku, abc_class, xyz_class, matrix_segment,
           mean_demand_exact AS hist_avg_daily, std_demand_exact AS hist_std_daily
    FROM public.vw_powerbi_abc_xyz_by_origin WHERE data_origin = 'unverified_warehouse'
),
base AS (
    SELECT l.product_key, l.date_key AS inventory_date_key, p.sku, p.product_name, p.category,
           s.abc_class, s.xyz_class, s.matrix_segment,
           l.full_date AS inventory_date, (t.d - l.full_date) AS inventory_age_days,
           l.stock_on_hand AS on_hand,
           COALESCE(l.lead_time_days, 3) AS lead_time_days, (l.lead_time_days IS NULL) AS lead_time_defaulted,
           CASE s.abc_class WHEN 'A' THEN 0.98 WHEN 'B' THEN 0.95 WHEN 'C' THEN 0.90 ELSE 0.95 END AS service_level,
           CASE s.abc_class WHEN 'A' THEN 2.054 WHEN 'B' THEN 1.645 WHEN 'C' THEN 1.282 ELSE 1.645 END AS z_value,
           COALESCE(s.hist_std_daily, 0) AS sigma_daily, s.hist_avg_daily,
           p.unit_cost AS reference_selling_price, t.d AS today
    FROM latest l
    JOIN public.Dim_Products p USING (product_key)
    CROSS JOIN today t
    LEFT JOIN seg s ON s.sku = p.sku
),
fc AS (
    SELECT b.product_key,
           SUM(f.predicted_quantity) FILTER (WHERE f.target_date >= b.today AND f.target_date < b.today + b.lead_time_days) AS forecast_lead_time,
           COUNT(*) FILTER (WHERE f.target_date >= b.today AND f.target_date < b.today + b.lead_time_days) AS forecast_days_in_lead_time,
           AVG(f.predicted_quantity) FILTER (WHERE f.target_date >= b.today AND f.target_date < b.today + 7) AS forecast_avg_daily_7d,
           SUM(f.predicted_quantity) FILTER (WHERE f.target_date >= b.today AND f.target_date < b.today + 7) AS forecast_next_7d,
           MAX(f.model_version) AS model_version
    FROM base b JOIN bi.forecast_daily f USING (product_key)
    GROUP BY b.product_key
),
calc AS (
    SELECT b.*, fc.forecast_next_7d, fc.model_version,
           CASE WHEN fc.forecast_days_in_lead_time = b.lead_time_days THEN 'ai_forecast'
                WHEN b.hist_avg_daily IS NOT NULL THEN 'historical_average'
                ELSE 'no_demand_data' END AS demand_source,
           CASE WHEN fc.forecast_days_in_lead_time = b.lead_time_days THEN fc.forecast_lead_time
                ELSE b.hist_avg_daily * b.lead_time_days END AS demand_lead_time,
           CASE WHEN fc.forecast_days_in_lead_time = b.lead_time_days THEN fc.forecast_avg_daily_7d
                ELSE b.hist_avg_daily END AS avg_daily_demand,
           CEIL(b.z_value * b.sigma_daily * SQRT(b.lead_time_days))::NUMERIC AS safety_stock,
           b.on_hand AS inventory_position
    FROM base b LEFT JOIN fc USING (product_key)
),
policy AS (
    SELECT c.*,
           CEIL(c.demand_lead_time + c.safety_stock) AS reorder_point,
           CEIL(c.demand_lead_time + c.safety_stock + COALESCE(c.avg_daily_demand, 0) * 7) AS order_up_to,
           ROUND(c.on_hand / NULLIF(c.avg_daily_demand, 0), 1) AS days_of_cover
    FROM calc c
),
decision AS (
    SELECT p.*,
           CASE WHEN p.demand_source = 'no_demand_data' THEN 'NO_DEMAND_DATA'
                WHEN p.on_hand <= p.safety_stock THEN 'CRITICAL'
                WHEN p.inventory_position <= p.reorder_point THEN 'REORDER'
                ELSE 'OK' END AS action_status,
           CASE WHEN p.demand_source <> 'no_demand_data' AND p.inventory_position <= p.reorder_point
                THEN GREATEST(0, p.order_up_to - p.inventory_position) ELSE 0 END AS recommended_order_qty,
           CASE WHEN p.days_of_cover IS NOT NULL THEN p.today + FLOOR(p.days_of_cover)::INT END AS projected_stockout_date
    FROM policy p
)
SELECT product_key, inventory_date_key, sku, product_name, category, abc_class, xyz_class, matrix_segment,
       inventory_date, inventory_age_days, (inventory_age_days > 2) AS inventory_stale,
       on_hand, inventory_position,
       lead_time_days, lead_time_defaulted, service_level, z_value,
       ROUND(sigma_daily, 2) AS sigma_daily, ROUND(avg_daily_demand, 2) AS avg_daily_demand,
       ROUND(demand_lead_time, 1) AS demand_lead_time, ROUND(forecast_next_7d, 1) AS forecast_next_7d,
       demand_source, model_version, safety_stock, reorder_point, order_up_to, days_of_cover,
       action_status, recommended_order_qty, projected_stockout_date,
       (projected_stockout_date - lead_time_days) AS order_by_date,
       CASE WHEN action_status IN ('CRITICAL', 'REORDER') AND projected_stockout_date - lead_time_days <= today THEN 'Đặt ngay'
            WHEN action_status IN ('CRITICAL', 'REORDER') THEN 'Đặt trong tuần'
            WHEN action_status = 'NO_DEMAND_DATA' THEN 'Thiếu dữ liệu nhu cầu'
            ELSE 'Chưa cần đặt' END AS urgency,
       reference_selling_price,
       recommended_order_qty * reference_selling_price AS order_value_at_selling_price,
       CASE WHEN demand_source = 'no_demand_data' THEN 'Chưa có lịch sử bán hay dự báo; không tự đề xuất.'
            -- Nối chuỗi thay cho FORMAT: driver coi ký tự phần trăm là tham số; CHR(37) là dấu phần trăm.
            ELSE 'Tồn hiện có ' || on_hand || ' '
                 || CASE WHEN inventory_position <= reorder_point THEN '≤' ELSE '>' END
                 || ' điểm đặt hàng ' || reorder_point || ' = nhu cầu ' || lead_time_days || ' ngày '
                 || ROUND(demand_lead_time, 0) || ' ('
                 || CASE demand_source WHEN 'ai_forecast' THEN 'dự báo AI' ELSE 'trung bình lịch sử' END
                 || ') + tồn an toàn ' || safety_stock || ' (mức phục vụ ' || ROUND(service_level * 100) || CHR(37) || ').'
       END AS reason,
       today AS evaluated_on
FROM decision;

-- Độ mới và độ tin cậy dữ liệu: một dòng cho trang "Độ tin cậy dữ liệu".
CREATE OR REPLACE VIEW bi.data_freshness AS
SELECT (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh') AS checked_at_vn,
       (SELECT MAX(loaded_at) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh' FROM public.Fact_Orders) AS last_order_loaded_vn,
       (SELECT ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(loaded_at) AT TIME ZONE 'UTC')) / 60.0, 1) FROM public.Fact_Orders) AS minutes_since_last_order_load,
       (SELECT MAX(create_time) FROM public.Fact_Orders) AS last_order_created_vn,
       (SELECT MAX(snapshot_at) AT TIME ZONE 'Asia/Ho_Chi_Minh' FROM public.Fact_Inventory_Daily) AS last_inventory_snapshot_vn,
       (SELECT MAX(full_date) FROM public.Warehouse_Daily_Coverage WHERE data_origin = 'warehouse_live' AND source_dataset = '') AS last_confirmed_day,
       ((NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE - 1 - (SELECT MAX(full_date) FROM public.Warehouse_Daily_Coverage
           WHERE data_origin = 'warehouse_live' AND source_dataset = '')) AS days_awaiting_confirmation,
       m.model_version, m.algorithm AS model_algorithm, m.model_stage, m.cv_wape AS model_backtest_wape,
       m.snapshot_at_vn AS last_forecast_run_vn, m.skus_model AS skus_forecast_by_model, m.skus_heuristic AS skus_forecast_by_heuristic,
       (SELECT COUNT(*) FROM bi.replenishment_recommendation WHERE inventory_stale) AS skus_with_stale_inventory,
       (SELECT COUNT(*) FROM bi.quality_order_header_conflicts) AS orders_with_header_conflict,
       (SELECT COUNT(*) FROM bi.quality_shop_identity) AS shop_identity_issues
FROM (SELECT 1) one LEFT JOIN bi.model_status m ON TRUE;

COMMENT ON SCHEMA bi IS 'BI contract v1: detail facts with shared keys, source-aware historical catalog, explicit quality flags.';
COMMENT ON VIEW bi.fact_order_line IS
    'Grain: source order line. Orders measure is DISTINCTCOUNT(order_business_key), not SUM of daily distinct counts. buyer_amount retains original source semantics.';
COMMENT ON VIEW bi.inventory_latest IS
    'Grain: newest available snapshot per live product, dates may differ across products. Never sum daily snapshots as current stock.';
