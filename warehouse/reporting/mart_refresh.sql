-- mart.refresh(p_full) — làm mới star schema mart từ kho tích hợp public (16/11/2026).
--
-- Gia tăng (mặc định): fact theo mốc public.*.loaded_at / forecast_id đã xử lý, chồng lấn 10 phút để
-- không sót giao dịch ghi muộn; dimension upsert toàn bộ (nhỏ). Đầy đủ (p_full = TRUE): nạp lại mọi
-- fact trừ fact_replenishment_daily (lịch sử đề xuất không tái tạo được). Dùng full sau các migration
-- sửa dữ liệu không đổi loaded_at (ví dụ gom shop).
-- Đề xuất đặt hàng: chỉ ghi đè dòng của ngày hôm nay (giờ Việt Nam); ngày trước giữ nguyên.
-- Thực tế của dự báo: ghi cho ngày mục tiêu đã xác nhận đủ dữ liệu (coverage warehouse_live).
-- Không dùng ký tự phần trăm trong tệp (psycopg2).

CREATE OR REPLACE FUNCTION mart.date_key(d DATE) RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$ SELECT (EXTRACT(YEAR FROM d) * 10000 + EXTRACT(MONTH FROM d) * 100 + EXTRACT(DAY FROM d))::INTEGER $$;

CREATE OR REPLACE FUNCTION mart.refresh(p_full BOOLEAN DEFAULT FALSE) RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_started      TIMESTAMPTZ := clock_timestamp();
    v_today        DATE := (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE;
    v_full         BOOLEAN := p_full;
    v_sales_since  TIMESTAMP;
    v_inv_since    TIMESTAMP;
    v_fc_since     BIGINT;
    v_start_date   DATE;
    v_end_date     DATE;
    v_rows         INTEGER;
    v_stats        JSONB := '{}'::JSONB;
BEGIN
    -- Một lượt làm mới tại một thời điểm (worker và batch-forecaster cùng gọi).
    PERFORM pg_advisory_xact_lock(2026111601);

    IF NOT v_full AND NOT EXISTS (SELECT 1 FROM mart.refresh_state WHERE name = 'sales_loaded_at') THEN
        v_full := TRUE;   -- lần đầu: nạp đầy đủ
    END IF;
    IF v_full THEN
        TRUNCATE mart.fact_sales_line, mart.fact_sales_order, mart.fact_inventory_daily, mart.fact_demand_forecast;
        v_sales_since := '-infinity'; v_inv_since := '-infinity'; v_fc_since := -1;
    ELSE
        v_sales_since := (SELECT value::TIMESTAMP FROM mart.refresh_state WHERE name = 'sales_loaded_at') - INTERVAL '10 minutes';
        v_inv_since := COALESCE((SELECT value::TIMESTAMP FROM mart.refresh_state WHERE name = 'inventory_loaded_at'), '-infinity'::TIMESTAMP) - INTERVAL '10 minutes';
        v_fc_since := COALESCE((SELECT value::BIGINT FROM mart.refresh_state WHERE name = 'forecast_id'), -1);
    END IF;

    -- ── dim_date: liên tục, phủ mọi ngày fact dùng tới ──
    SELECT LEAST(DATE '2025-01-01',
                 (SELECT MIN(create_time)::DATE FROM public.Fact_Orders),
                 (SELECT MIN(target_date) FROM public.Fact_Forecast_Predictions),
                 (SELECT MIN(history_end_date) FROM public.Fact_Forecast_Predictions),
                 (SELECT MIN(d.full_date) FROM public.Fact_Inventory_Daily i JOIN public.Dim_Dates d USING (date_key))),
           GREATEST(DATE '2027-12-31', v_today + 60,
                 (SELECT MAX(target_date) FROM public.Fact_Forecast_Predictions),
                 (SELECT MAX(GREATEST(create_time, completed_time))::DATE FROM public.Fact_Orders))
      INTO v_start_date, v_end_date;
    INSERT INTO mart.dim_date
    SELECT mart.date_key(d), d, EXTRACT(DAY FROM d), EXTRACT(ISODOW FROM d),
           (ARRAY['Thứ Hai','Thứ Ba','Thứ Tư','Thứ Năm','Thứ Sáu','Thứ Bảy','Chủ Nhật'])[EXTRACT(ISODOW FROM d)::INT],
           EXTRACT(WEEK FROM d), EXTRACT(MONTH FROM d), 'Tháng ' || EXTRACT(MONTH FROM d)::INT,
           EXTRACT(QUARTER FROM d), EXTRACT(YEAR FROM d),
           (EXTRACT(YEAR FROM d) * 100 + EXTRACT(MONTH FROM d))::INT,
           LPAD(EXTRACT(MONTH FROM d)::INT::TEXT, 2, '0') || '/' || EXTRACT(YEAR FROM d)::INT,
           EXTRACT(ISODOW FROM d) IN (6, 7), EXTRACT(DAY FROM d) = EXTRACT(MONTH FROM d),
           EXISTS (SELECT 1 FROM public.Warehouse_Daily_Coverage c
                   WHERE c.data_origin = 'warehouse_live' AND c.source_dataset = '' AND c.full_date = d)
    FROM generate_series(v_start_date, v_end_date, INTERVAL '1 day') g(ts), LATERAL (SELECT g.ts::DATE AS d) x
    ON CONFLICT (date_key) DO UPDATE SET is_data_confirmed = EXCLUDED.is_data_confirmed
    WHERE mart.dim_date.is_data_confirmed IS DISTINCT FROM EXCLUDED.is_data_confirmed;
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('dim_date', v_rows);

    -- ── dim_product: thuộc tính SCD1, nhóm ABC/XYZ trên toàn bộ đơn của kho, lead time mới nhất ──
    INSERT INTO mart.dim_product (product_key, sku, product_name, category, abc_class, xyz_class, matrix_segment,
                                  service_level, reference_selling_price, current_lead_time_days, updated_at)
    SELECT p.product_key, p.sku, COALESCE(NULLIF(p.product_name, ''), p.sku), COALESCE(NULLIF(p.category, ''), 'Chưa phân loại'),
           COALESCE(s.abc_class, 'Chưa phân nhóm'), COALESCE(s.xyz_class, 'Chưa phân nhóm'), COALESCE(s.matrix_segment, 'Chưa phân nhóm'),
           CASE s.abc_class WHEN 'A' THEN 0.98 WHEN 'B' THEN 0.95 WHEN 'C' THEN 0.90 ELSE 0.95 END,
           p.unit_cost, lt.lead_time_days, NOW()
    FROM public.Dim_Products p
    LEFT JOIN public.vw_powerbi_abc_xyz_by_origin s ON s.sku = p.sku AND s.data_origin = 'unverified_warehouse'
    LEFT JOIN LATERAL (SELECT i.lead_time_days FROM public.Fact_Inventory_Daily i JOIN public.Dim_Dates d USING (date_key)
                       WHERE i.product_key = p.product_key AND i.lead_time_days IS NOT NULL
                       ORDER BY d.full_date DESC, i.snapshot_at DESC NULLS LAST LIMIT 1) lt ON TRUE
    ON CONFLICT (product_key) DO UPDATE SET
        sku = EXCLUDED.sku, product_name = EXCLUDED.product_name, category = EXCLUDED.category,
        abc_class = EXCLUDED.abc_class, xyz_class = EXCLUDED.xyz_class, matrix_segment = EXCLUDED.matrix_segment,
        service_level = EXCLUDED.service_level, reference_selling_price = EXCLUDED.reference_selling_price,
        current_lead_time_days = EXCLUDED.current_lead_time_days, updated_at = NOW()
    WHERE (mart.dim_product.sku, mart.dim_product.product_name, mart.dim_product.category, mart.dim_product.abc_class,
           mart.dim_product.xyz_class, mart.dim_product.matrix_segment, mart.dim_product.service_level,
           mart.dim_product.reference_selling_price, mart.dim_product.current_lead_time_days)
          IS DISTINCT FROM
          (EXCLUDED.sku, EXCLUDED.product_name, EXCLUDED.category, EXCLUDED.abc_class, EXCLUDED.xyz_class,
           EXCLUDED.matrix_segment, EXCLUDED.service_level, EXCLUDED.reference_selling_price, EXCLUDED.current_lead_time_days);
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('dim_product', v_rows);

    -- ── dim_shop, dim_geography, dim_payment, dim_carrier ──
    INSERT INTO mart.dim_shop
    SELECT s.shop_key, COALESCE(NULLIF(s.shop_id, ''), '(trống)'), s.shop_name,
           COALESCE((SELECT platform_key FROM mart.dim_platform WHERE platform_code = s.platform), -1)
    FROM public.Dim_Shops s
    ON CONFLICT (shop_key) DO UPDATE SET shop_id = EXCLUDED.shop_id, shop_name = EXCLUDED.shop_name, platform_key = EXCLUDED.platform_key
    WHERE (mart.dim_shop.shop_id, mart.dim_shop.shop_name, mart.dim_shop.platform_key)
          IS DISTINCT FROM (EXCLUDED.shop_id, EXCLUDED.shop_name, EXCLUDED.platform_key);
    -- Shop đã bị gom/xóa ở public và không còn fact nào tham chiếu thì xóa khỏi mart.
    DELETE FROM mart.dim_shop d WHERE d.shop_key <> -1
      AND NOT EXISTS (SELECT 1 FROM public.Dim_Shops s WHERE s.shop_key = d.shop_key)
      AND NOT EXISTS (SELECT 1 FROM mart.fact_sales_order f WHERE f.shop_key = d.shop_key)
      AND NOT EXISTS (SELECT 1 FROM mart.fact_sales_line f WHERE f.shop_key = d.shop_key);

    INSERT INTO mart.dim_geography
    SELECT g.geo_key, COALESCE(g.region, 'Không xác định'), g.state, COALESCE(g.city, 'Không xác định'),
           COALESCE(g.district, 'Không xác định'), COALESCE(g.country, 'VN')
    FROM public.Dim_Geography g
    ON CONFLICT (geo_key) DO UPDATE SET region = EXCLUDED.region, state = EXCLUDED.state, city = EXCLUDED.city,
        district = EXCLUDED.district, country = EXCLUDED.country
    WHERE (mart.dim_geography.region, mart.dim_geography.state, mart.dim_geography.city, mart.dim_geography.district)
          IS DISTINCT FROM (EXCLUDED.region, EXCLUDED.state, EXCLUDED.city, EXCLUDED.district);

    INSERT INTO mart.dim_payment SELECT payment_key, payment_method, COALESCE(is_cod, FALSE) FROM public.Dim_Payment
    ON CONFLICT (payment_key) DO UPDATE SET payment_method = EXCLUDED.payment_method, is_cod = EXCLUDED.is_cod
    WHERE (mart.dim_payment.payment_method, mart.dim_payment.is_cod) IS DISTINCT FROM (EXCLUDED.payment_method, EXCLUDED.is_cod);

    INSERT INTO mart.dim_carrier SELECT carrier_key, carrier_name FROM public.Dim_Carriers
    ON CONFLICT (carrier_key) DO UPDATE SET carrier_name = EXCLUDED.carrier_name
    WHERE mart.dim_carrier.carrier_name IS DISTINCT FROM EXCLUDED.carrier_name;

    -- Trạng thái mới xuất hiện ở nguồn: thêm vào nhóm 'Khác', không làm hỏng fact.
    INSERT INTO mart.dim_order_status (status_key, order_status, status_group, funnel_order, is_final)
    SELECT (SELECT MAX(status_key) FROM mart.dim_order_status) + ROW_NUMBER() OVER (ORDER BY s.order_status),
           s.order_status, 'Khác', 8, FALSE
    FROM (SELECT DISTINCT order_status FROM public.Fact_Orders) s
    WHERE NOT EXISTS (SELECT 1 FROM mart.dim_order_status d WHERE d.order_status = s.order_status);

    -- ── dim_model: mỗi (thuật toán, phiên bản, nguồn huấn luyện) từng phát dự báo ──
    INSERT INTO mart.dim_model (model_key, model_name, model_version, training_data_origin, version_label)
    SELECT nextval('mart.dim_model_key_seq'), m.model_name, m.model_version, m.data_origin,
           'v' || m.model_version || ' · ' || m.model_name
    FROM (SELECT DISTINCT model_name, model_version, data_origin FROM public.Fact_Forecast_Predictions) m
    WHERE NOT EXISTS (SELECT 1 FROM mart.dim_model d WHERE d.model_name = m.model_name
                      AND d.model_version = m.model_version AND d.training_data_origin = m.data_origin);
    UPDATE mart.dim_model d SET
        first_forecast_at = f.first_at, last_forecast_at = f.last_at,
        model_stage = COALESCE(s.model_stage, d.model_stage), cv_wape = COALESCE(s.cv_wape, d.cv_wape),
        cv_bias = COALESCE(s.cv_bias, d.cv_bias), evaluation_protocol = COALESCE(s.evaluation_protocol, d.evaluation_protocol),
        is_serving = EXISTS (SELECT 1 FROM (SELECT model_version, algorithm, training_data_origin
                                            FROM public.Model_Serving_Snapshot ORDER BY snapshot_id DESC LIMIT 1) cur
                             WHERE cur.model_version = d.model_version AND cur.algorithm = d.model_name
                               AND cur.training_data_origin = d.training_data_origin)
    FROM (SELECT model_name, model_version, data_origin,
                 MIN(forecast_generated_at) AT TIME ZONE 'Asia/Ho_Chi_Minh' first_at,
                 MAX(forecast_generated_at) AT TIME ZONE 'Asia/Ho_Chi_Minh' last_at
          FROM public.Fact_Forecast_Predictions GROUP BY 1, 2, 3) f
    LEFT JOIN LATERAL (SELECT * FROM public.Model_Serving_Snapshot ms
                       WHERE ms.model_version = f.model_version AND ms.algorithm = f.model_name
                         AND ms.training_data_origin = f.data_origin
                       ORDER BY ms.snapshot_id DESC LIMIT 1) s ON TRUE
    WHERE d.model_name = f.model_name AND d.model_version = f.model_version AND d.training_data_origin = f.data_origin;

    -- ── fact_sales_line ──
    INSERT INTO mart.fact_sales_line
    SELECT f.order_fact_id, f.order_id, mart.date_key(f.create_time::DATE), f.product_key,
           COALESCE(pl.platform_key, -1), COALESCE(sh.shop_key, -1), COALESCE(ge.geo_key, -1), COALESCE(st.status_key, -1),
           f.is_cancelled, f.quantity, CASE WHEN f.is_cancelled THEN 0 ELSE f.quantity END,
           f.original_price, COALESCE(f.buyer_total_amount, 0),
           CASE WHEN f.is_cancelled THEN 0 ELSE COALESCE(f.buyer_total_amount, 0) END,
           COALESCE(f.seller_discount, 0) + COALESCE(f.platform_discount, 0) + COALESCE(f.voucher_total, 0),
           COALESCE(f.commission_fee, 0) + COALESCE(f.service_fee, 0) + COALESCE(f.transaction_fee, 0),
           f.create_time, f.loaded_at
    FROM public.Fact_Orders f
    LEFT JOIN mart.dim_platform pl ON pl.platform_code = f.platform
    LEFT JOIN mart.dim_shop sh ON sh.shop_key = f.shop_key
    LEFT JOIN mart.dim_geography ge ON ge.geo_key = f.geo_key
    LEFT JOIN mart.dim_order_status st ON st.order_status = f.order_status
    WHERE f.loaded_at > v_sales_since OR f.loaded_at IS NULL AND v_full
    ON CONFLICT (sales_line_key) DO UPDATE SET
        order_id = EXCLUDED.order_id, order_date_key = EXCLUDED.order_date_key, product_key = EXCLUDED.product_key,
        platform_key = EXCLUDED.platform_key, shop_key = EXCLUDED.shop_key, geo_key = EXCLUDED.geo_key,
        status_key = EXCLUDED.status_key, is_cancelled = EXCLUDED.is_cancelled, quantity = EXCLUDED.quantity,
        net_quantity = EXCLUDED.net_quantity, unit_price = EXCLUDED.unit_price, gross_amount = EXCLUDED.gross_amount,
        net_amount = EXCLUDED.net_amount, discount_amount = EXCLUDED.discount_amount, platform_fees = EXCLUDED.platform_fees,
        create_time = EXCLUDED.create_time, source_loaded_at = EXCLUDED.source_loaded_at
    WHERE mart.fact_sales_line.source_loaded_at IS DISTINCT FROM EXCLUDED.source_loaded_at;
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('fact_sales_line', v_rows);

    -- ── fact_sales_order (accumulating snapshot) cho các đơn có dòng mới/đổi ──
    INSERT INTO mart.fact_sales_order
    SELECT MIN(f.order_fact_id), f.order_id, COALESCE(MIN(pl.platform_key), -1),
           COALESCE((ARRAY_AGG(sh.shop_key ORDER BY f.order_fact_id))[1], -1),
           COALESCE((ARRAY_AGG(ge.geo_key ORDER BY f.order_fact_id))[1], -1),
           COALESCE((ARRAY_AGG(pa.payment_key ORDER BY f.order_fact_id))[1], -1),
           -- -2 "Chưa giao vận chuyển": đơn chưa từng giao — không dòng nào ở trạng thái giao/đã giao/hoàn tất/hoàn trả
           --    (funnel_order 3, 4, 5, 7) và không có thời điểm giao/hoàn tất;
           -- -1 "Không xác định": đơn đã giao/hoàn tất (theo trạng thái HOẶC mốc thời gian) mà nguồn thiếu đơn vị vận chuyển.
           CASE WHEN (ARRAY_AGG(ca.carrier_key ORDER BY f.order_fact_id) FILTER (WHERE ca.carrier_key IS NOT NULL))[1] IS NOT NULL
                THEN (ARRAY_AGG(ca.carrier_key ORDER BY f.order_fact_id) FILTER (WHERE ca.carrier_key IS NOT NULL))[1]
                WHEN MIN(f.shipped_time) IS NULL AND MAX(f.completed_time) IS NULL
                     AND NOT COALESCE(BOOL_OR(st.funnel_order IN (3, 4, 5, 7)), FALSE) THEN -2
                ELSE -1 END,
           COALESCE(CASE WHEN BOOL_AND(f.is_cancelled) THEN (SELECT status_key FROM mart.dim_order_status WHERE order_status = 'CANCELLED')
                         ELSE (ARRAY_AGG(st.status_key ORDER BY f.is_cancelled, f.order_fact_id))[1] END, -1),
           mart.date_key(MIN(f.create_time)::DATE),
           CASE WHEN MAX(f.completed_time) IS NOT NULL THEN mart.date_key(MAX(f.completed_time)::DATE) END,
           MIN(f.create_time), MIN(f.pay_time), MIN(f.shipped_time), MAX(f.completed_time), MAX(f.cancel_time),
           EXTRACT(HOUR FROM MIN(f.create_time)),
           ROUND(EXTRACT(EPOCH FROM (MIN(f.pay_time) - MIN(f.create_time))) / 3600.0, 1),
           ROUND(EXTRACT(EPOCH FROM (MIN(f.shipped_time) - MIN(f.create_time))) / 3600.0, 1),
           ROUND(EXTRACT(EPOCH FROM (MAX(f.completed_time) - MIN(f.create_time))) / 3600.0, 1),
           BOOL_AND(f.is_cancelled), COUNT(*), SUM(f.quantity),
           COALESCE(SUM(f.quantity) FILTER (WHERE NOT f.is_cancelled), 0),
           COALESCE(SUM(f.buyer_total_amount), 0),
           COALESCE(SUM(f.buyer_total_amount) FILTER (WHERE NOT f.is_cancelled), 0),
           COALESCE(SUM(COALESCE(f.seller_discount, 0) + COALESCE(f.platform_discount, 0) + COALESCE(f.voucher_total, 0)), 0),
           COALESCE(SUM(COALESCE(f.commission_fee, 0) + COALESCE(f.service_fee, 0) + COALESCE(f.transaction_fee, 0)), 0),
           COALESCE(SUM(f.shipping_fee), 0),
           COUNT(DISTINCT f.date_key) > 1 OR COUNT(DISTINCT COALESCE(f.shop_key, 0)) > 1
             OR COUNT(DISTINCT COALESCE(f.geo_key, 0)) > 1 OR COUNT(DISTINCT f.is_cancelled) > 1
    FROM public.Fact_Orders f
    JOIN (SELECT DISTINCT platform, order_id FROM public.Fact_Orders
          WHERE loaded_at > v_sales_since OR loaded_at IS NULL AND v_full) t USING (platform, order_id)
    LEFT JOIN mart.dim_platform pl ON pl.platform_code = f.platform
    LEFT JOIN mart.dim_shop sh ON sh.shop_key = f.shop_key
    LEFT JOIN mart.dim_geography ge ON ge.geo_key = f.geo_key
    LEFT JOIN mart.dim_payment pa ON pa.payment_key = f.payment_key
    LEFT JOIN mart.dim_carrier ca ON ca.carrier_key = f.carrier_key
    LEFT JOIN mart.dim_order_status st ON st.order_status = f.order_status
    GROUP BY f.platform, f.order_id
    ON CONFLICT (platform_key, order_id) DO UPDATE SET
        shop_key = EXCLUDED.shop_key, geo_key = EXCLUDED.geo_key, payment_key = EXCLUDED.payment_key,
        carrier_key = EXCLUDED.carrier_key, status_key = EXCLUDED.status_key, order_date_key = EXCLUDED.order_date_key,
        completed_date_key = EXCLUDED.completed_date_key, create_time = EXCLUDED.create_time, pay_time = EXCLUDED.pay_time,
        shipped_time = EXCLUDED.shipped_time, completed_time = EXCLUDED.completed_time, cancel_time = EXCLUDED.cancel_time,
        order_hour = EXCLUDED.order_hour, hours_to_pay = EXCLUDED.hours_to_pay, hours_to_ship = EXCLUDED.hours_to_ship,
        hours_to_complete = EXCLUDED.hours_to_complete, is_cancelled = EXCLUDED.is_cancelled, line_count = EXCLUDED.line_count,
        gross_units = EXCLUDED.gross_units, net_units = EXCLUDED.net_units, gross_amount = EXCLUDED.gross_amount,
        net_amount = EXCLUDED.net_amount, discount_amount = EXCLUDED.discount_amount, platform_fees = EXCLUDED.platform_fees,
        shipping_fee = EXCLUDED.shipping_fee, has_header_conflict = EXCLUDED.has_header_conflict;
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('fact_sales_order', v_rows);

    -- ── fact_inventory_daily ──
    INSERT INTO mart.fact_inventory_daily
    SELECT i.product_key, mart.date_key(d.full_date), i.stock_on_hand, i.lead_time_days,
           i.snapshot_at AT TIME ZONE 'Asia/Ho_Chi_Minh',   -- mart lưu giờ Việt Nam, không múi giờ
           i.snapshot_at IS NOT NULL, i.loaded_at
    FROM public.Fact_Inventory_Daily i JOIN public.Dim_Dates d USING (date_key)
    WHERE i.loaded_at > v_inv_since OR i.loaded_at IS NULL AND v_full
    ON CONFLICT (product_key, snapshot_date_key) DO UPDATE SET
        on_hand = EXCLUDED.on_hand, lead_time_days = EXCLUDED.lead_time_days, snapshot_at = EXCLUDED.snapshot_at,
        snapshot_time_known = EXCLUDED.snapshot_time_known, source_loaded_at = EXCLUDED.source_loaded_at
    WHERE mart.fact_inventory_daily.source_loaded_at IS DISTINCT FROM EXCLUDED.source_loaded_at;
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('fact_inventory_daily', v_rows);

    -- ── fact_demand_forecast: thêm dự báo mới (bảng nguồn chỉ thêm, không sửa) ──
    INSERT INTO mart.fact_demand_forecast (forecast_key, product_key, target_date_key, issued_date_key, model_key,
        horizon_days, predicted_quantity, lower_bound, upper_bound, forecast_generated_at)
    SELECT fp.forecast_id, fp.product_key, mart.date_key(fp.target_date),
           mart.date_key(COALESCE(fp.history_end_date, (fp.forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE)),
           COALESCE(dm.model_key, -1),
           GREATEST(1, fp.target_date - COALESCE(fp.history_end_date, (fp.forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE)),
           fp.predicted_quantity, fp.lower_bound, fp.upper_bound, fp.forecast_generated_at AT TIME ZONE 'Asia/Ho_Chi_Minh'
    FROM public.Fact_Forecast_Predictions fp
    LEFT JOIN mart.dim_model dm ON dm.model_name = fp.model_name AND dm.model_version = fp.model_version
                               AND dm.training_data_origin = fp.data_origin
    WHERE fp.forecast_id > v_fc_since
    ON CONFLICT (forecast_key) DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('fact_demand_forecast', v_rows);

    -- Cờ bản mới nhất cho mỗi SKU × ngày mục tiêu trong cùng nguồn huấn luyện.
    UPDATE mart.fact_demand_forecast f SET is_latest_for_target = r.is_latest
    FROM (SELECT fd.forecast_key, ROW_NUMBER() OVER (PARTITION BY fd.product_key, fd.target_date_key, dm.training_data_origin
                                                     ORDER BY fd.forecast_generated_at DESC, fd.forecast_key DESC) = 1 AS is_latest
          FROM mart.fact_demand_forecast fd JOIN mart.dim_model dm USING (model_key)) r
    WHERE f.forecast_key = r.forecast_key AND f.is_latest_for_target IS DISTINCT FROM r.is_latest;

    -- Thực tế cho dự báo trên kho streaming khi ngày mục tiêu đã xác nhận đủ dữ liệu (chấm lại 7 ngày gần nhất vì có thể đến muộn).
    UPDATE mart.fact_demand_forecast f SET
        actual_quantity = a.actual, actual_complete = TRUE, absolute_error = ABS(f.predicted_quantity - a.actual)
    FROM (SELECT fd.forecast_key,
                 COALESCE((SELECT SUM(s.net_quantity) FROM mart.fact_sales_line s
                           WHERE s.product_key = fd.product_key AND s.order_date_key = fd.target_date_key), 0)::NUMERIC AS actual
          FROM mart.fact_demand_forecast fd
          JOIN mart.dim_model dm USING (model_key)
          JOIN mart.dim_date dd ON dd.date_key = fd.target_date_key
          WHERE dm.training_data_origin = 'warehouse_live' AND dd.is_data_confirmed
            AND (NOT fd.actual_complete OR dd.full_date >= v_today - 7 OR v_full)) a
    WHERE f.forecast_key = a.forecast_key
      AND (f.actual_quantity, f.actual_complete) IS DISTINCT FROM (a.actual, TRUE);
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('forecast_actuals_scored', v_rows);

    -- ── fact_replenishment_daily: chỉ dòng của hôm nay (logic SS/ROP duy nhất trong bi.replenishment_recommendation) ──
    INSERT INTO mart.fact_replenishment_daily
    SELECT r.product_key, mart.date_key(v_today),
           -- -2 "Không dùng dự báo AI" khi đề xuất dựa trên trung bình lịch sử hoặc không có dữ liệu nhu cầu;
           -- -1 khi dùng dự báo AI nhưng không tìm thấy phiên bản.
           CASE WHEN r.demand_source <> 'ai_forecast' THEN -2
                ELSE COALESCE((SELECT dm.model_key FROM mart.dim_model dm
                               WHERE dm.model_version = r.model_version AND dm.training_data_origin = 'warehouse_live'
                               ORDER BY dm.last_forecast_at DESC NULLS LAST LIMIT 1), -1) END,
           mart.date_key(r.inventory_date), r.inventory_age_days, r.on_hand, r.lead_time_days, r.lead_time_defaulted,
           r.service_level, r.sigma_daily, r.avg_daily_demand, r.demand_lead_time, r.forecast_next_7d, r.demand_source,
           r.safety_stock, r.reorder_point, r.order_up_to, r.days_of_cover, r.action_status, r.urgency,
           r.recommended_order_qty, r.projected_stockout_date, r.order_by_date, r.order_value_at_selling_price,
           r.reason, NOW()
    FROM bi.replenishment_recommendation r
    WHERE EXISTS (SELECT 1 FROM mart.dim_product p WHERE p.product_key = r.product_key)
    ON CONFLICT (product_key, evaluation_date_key) DO UPDATE SET
        model_key = EXCLUDED.model_key, inventory_date_key = EXCLUDED.inventory_date_key,
        inventory_age_days = EXCLUDED.inventory_age_days, on_hand = EXCLUDED.on_hand,
        lead_time_days = EXCLUDED.lead_time_days, lead_time_defaulted = EXCLUDED.lead_time_defaulted,
        service_level = EXCLUDED.service_level, sigma_daily = EXCLUDED.sigma_daily,
        avg_daily_demand = EXCLUDED.avg_daily_demand, demand_lead_time = EXCLUDED.demand_lead_time,
        forecast_next_7d = EXCLUDED.forecast_next_7d, demand_source = EXCLUDED.demand_source,
        safety_stock = EXCLUDED.safety_stock, reorder_point = EXCLUDED.reorder_point, order_up_to = EXCLUDED.order_up_to,
        days_of_cover = EXCLUDED.days_of_cover, action_status = EXCLUDED.action_status, urgency = EXCLUDED.urgency,
        recommended_order_qty = EXCLUDED.recommended_order_qty, projected_stockout_date = EXCLUDED.projected_stockout_date,
        order_by_date = EXCLUDED.order_by_date, order_value_at_selling_price = EXCLUDED.order_value_at_selling_price,
        reason = EXCLUDED.reason, refreshed_at = NOW();
    GET DIAGNOSTICS v_rows = ROW_COUNT; v_stats := v_stats || jsonb_build_object('fact_replenishment_today', v_rows);

    -- ── mốc gia tăng ──
    INSERT INTO mart.refresh_state VALUES
        ('sales_loaded_at', COALESCE((SELECT MAX(loaded_at) FROM public.Fact_Orders), '1900-01-01')::TEXT),
        ('inventory_loaded_at', COALESCE((SELECT MAX(loaded_at) FROM public.Fact_Inventory_Daily), '1900-01-01')::TEXT),
        ('forecast_id', COALESCE((SELECT MAX(forecast_id) FROM public.Fact_Forecast_Predictions), -1)::TEXT)
    ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value;

    v_stats := v_stats || jsonb_build_object('mode', CASE WHEN v_full THEN 'full' ELSE 'incremental' END,
                                             'evaluation_date', v_today,
                                             'seconds', ROUND(EXTRACT(EPOCH FROM clock_timestamp() - v_started)::NUMERIC, 2));
    INSERT INTO mart.refresh_log (started_at, finished_at, mode, stats)
    VALUES (v_started, clock_timestamp(), CASE WHEN v_full THEN 'full' ELSE 'incremental' END, v_stats);
    RETURN v_stats;
END;
$$;
