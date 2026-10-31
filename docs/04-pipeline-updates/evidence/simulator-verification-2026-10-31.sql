-- Read-only evidence queries; run against ecom_warehouse.
BEGIN READ ONLY;
-- orders
SELECT COUNT(*) AS total_orders, COUNT(*) FILTER (WHERE order_id LIKE 'SIM26YTD-%') AS simulated_orders, COUNT(*) FILTER (WHERE order_id NOT LIKE 'SIM26YTD-%') AS preserved_orders FROM fact_orders;

-- coverage
SELECT MIN(d.full_date) AS first_date, MAX(d.full_date) AS last_date, COUNT(DISTINCT d.full_date) AS days,COUNT(DISTINCT f.order_id) AS distinct_order_ids,COUNT(DISTINCT f.product_key) AS skus FROM fact_orders f JOIN dim_dates d USING(date_key) WHERE f.order_id LIKE 'SIM26YTD-%';

-- inventory
SELECT COUNT(*) AS total_rows,COUNT(*) FILTER(WHERE inventory_fact_id<=51) AS preserved_rows,COUNT(DISTINCT d.full_date) AS days FROM fact_inventory_daily f JOIN dim_dates d USING(date_key);

-- missing_days
SELECT COUNT(*) AS count FROM generate_series('2026-01-01'::date,'2026-10-31'::date,'1 day') g(day) WHERE NOT EXISTS (SELECT 1 FROM fact_orders f JOIN dim_dates d USING(date_key) WHERE f.order_id LIKE 'SIM26YTD-%' AND d.full_date=g.day::date);

-- invalid_transitions
SELECT COUNT(*) FILTER (WHERE GREATEST(create_time,pay_time,shipped_time,completed_time,cancel_time) > '2026-10-31 23:31:20.659634'::timestamp) AS after_cutoff,COUNT(*) FILTER(WHERE order_status='COMPLETED' AND completed_time IS NULL) AS missing_completed_time FROM fact_orders WHERE order_id LIKE 'SIM26YTD-%';

-- fingerprint_orders
SELECT COUNT(*) AS count,md5(COALESCE(string_agg(to_jsonb(t)::text,'' ORDER BY order_fact_id),'')) AS hash FROM fact_orders t WHERE order_id NOT LIKE 'SIM26YTD-%';

-- fingerprint_inventory
SELECT COUNT(*) AS count,md5(COALESCE(string_agg(to_jsonb(t)::text,'' ORDER BY inventory_fact_id),'')) AS hash FROM fact_inventory_daily t WHERE inventory_fact_id<=51;

-- fingerprint_products
SELECT COUNT(*) AS count,md5(COALESCE(string_agg(to_jsonb(t)::text,'' ORDER BY product_key),'')) AS hash FROM dim_products t WHERE TRUE;

-- fingerprint_shops
SELECT COUNT(*) AS count,md5(COALESCE(string_agg(to_jsonb(t)::text,'' ORDER BY shop_key),'')) AS hash FROM dim_shops t WHERE TRUE;
COMMIT;
