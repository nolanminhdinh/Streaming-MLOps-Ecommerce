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
    d.day,
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
    d.full_date, d.year, d.month, d.day, d.day_name, d.is_weekend, d.is_mega_sale, s.platform;


-- 2. View Ma trận 9 ô ABC/XYZ (ABC/XYZ Segmentation Matrix)
CREATE OR REPLACE VIEW vw_powerbi_abc_xyz_matrix AS
WITH sku_revenue AS (
    SELECT
        p.sku,
        p.product_name,
        p.category,
        SUM(f.buyer_total_amount) AS revenue,
        SUM(f.quantity)           AS total_units,
        COUNT(DISTINCT f.date_key) AS active_days
    FROM Fact_Orders f
    JOIN Dim_Products p ON f.product_key = p.product_key
    WHERE f.is_cancelled = FALSE
    GROUP BY p.sku, p.product_name, p.category
),
sku_cumulative AS (
    SELECT
        sku,
        product_name,
        category,
        revenue,
        total_units,
        SUM(revenue) OVER (ORDER BY revenue DESC) AS cum_revenue,
        SUM(revenue) OVER ()                      AS total_revenue
    FROM sku_revenue
),
sku_abc AS (
    SELECT
        sku,
        product_name,
        category,
        revenue,
        total_units,
        ROUND((cum_revenue / NULLIF(total_revenue, 0)) * 100, 2) AS cum_share_pct,
        CASE
            WHEN (cum_revenue / NULLIF(total_revenue, 0)) <= 0.80 THEN 'A'
            WHEN (cum_revenue / NULLIF(total_revenue, 0)) <= 0.95 THEN 'B'
            ELSE 'C'
        END AS abc_class
    FROM sku_cumulative
),
sku_daily_stats AS (
    SELECT
        p.sku,
        AVG(daily_q) AS mean_demand,
        COALESCE(STDDEV_SAMP(daily_q), 0) AS std_demand
    FROM (
        SELECT f.product_key, f.date_key, SUM(f.quantity) AS daily_q
        FROM Fact_Orders f
        WHERE f.is_cancelled = FALSE
        GROUP BY f.product_key, f.date_key
    ) d_agg
    JOIN Dim_Products p ON d_agg.product_key = p.product_key
    GROUP BY p.sku
)
SELECT
    a.sku,
    a.product_name,
    a.category,
    a.revenue,
    a.total_units,
    a.cum_share_pct,
    a.abc_class,
    ROUND(s.mean_demand, 2) AS avg_daily_demand,
    ROUND(s.std_demand, 2)  AS std_daily_demand,
    ROUND((s.std_demand / NULLIF(s.mean_demand, 0)), 2) AS cv_volatility,
    CASE
        WHEN (s.std_demand / NULLIF(s.mean_demand, 0)) < 0.50 THEN 'X'
        WHEN (s.std_demand / NULLIF(s.mean_demand, 0)) <= 1.00 THEN 'Y'
        ELSE 'Z'
    END AS xyz_class,
    (a.abc_class || CASE
        WHEN (s.std_demand / NULLIF(s.mean_demand, 0)) < 0.50 THEN 'X'
        WHEN (s.std_demand / NULLIF(s.mean_demand, 0)) <= 1.00 THEN 'Y'
        ELSE 'Z'
    END) AS matrix_segment
FROM sku_abc a
LEFT JOIN sku_daily_stats s ON a.sku = s.sku;


-- 3. View Giám sát Tồn kho & Cảnh báo Nhập hàng (Inventory Health & ROP Alerts)
CREATE OR REPLACE VIEW vw_powerbi_inventory_health AS
WITH latest_inv AS (
    SELECT
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
        avg_daily_demand,
        std_daily_demand,
        matrix_segment,
        -- Safety Stock = ceil(1.645 * std * sqrt(3))
        CEIL(1.645 * std_daily_demand * SQRT(3)) AS safety_stock,
        -- ROP = ceil(avg * 3 + SS)
        CEIL((avg_daily_demand * 3) + CEIL(1.645 * std_daily_demand * SQRT(3))) AS reorder_point
    FROM vw_powerbi_abc_xyz_matrix
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
    inv.inventory_date AS last_audited_date
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
