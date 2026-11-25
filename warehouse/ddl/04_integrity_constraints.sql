-- Additive hardening. Validation fails atomically if existing rows violate a rule.
-- No facts are deleted, re-keyed or rewritten.
ALTER TABLE public.Fact_Orders ALTER COLUMN product_key SET NOT NULL;
ALTER TABLE public.Fact_Orders ALTER COLUMN date_key SET NOT NULL;
ALTER TABLE public.Fact_Orders ALTER COLUMN is_cancelled SET NOT NULL;
ALTER TABLE public.Fact_Inventory_Daily ALTER COLUMN product_key SET NOT NULL;
ALTER TABLE public.Fact_Inventory_Daily ALTER COLUMN date_key SET NOT NULL;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_orders'::regclass
                   AND conname='ck_orders_quantity_positive') THEN
        ALTER TABLE public.Fact_Orders ADD CONSTRAINT ck_orders_quantity_positive CHECK (quantity > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_orders'::regclass
                   AND conname='ck_orders_supported_platform') THEN
        ALTER TABLE public.Fact_Orders ADD CONSTRAINT ck_orders_supported_platform
            CHECK (platform IN ('shopee', 'tiktok'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_orders'::regclass
                   AND conname='ck_orders_amounts_valid') THEN
        ALTER TABLE public.Fact_Orders ADD CONSTRAINT ck_orders_amounts_valid CHECK (
            (original_price IS NULL OR (original_price >= 0 AND original_price <> 'NaN'::NUMERIC)) AND
            (subtotal IS NULL OR (subtotal >= 0 AND subtotal <> 'NaN'::NUMERIC)) AND
            (buyer_total_amount IS NULL OR (buyer_total_amount >= 0 AND buyer_total_amount <> 'NaN'::NUMERIC))
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_inventory_daily'::regclass
                   AND conname='ck_inventory_nonnegative') THEN
        ALTER TABLE public.Fact_Inventory_Daily ADD CONSTRAINT ck_inventory_nonnegative CHECK (
            stock_on_hand >= 0 AND (safety_stock IS NULL OR safety_stock >= 0)
            AND (reorder_point IS NULL OR reorder_point >= 0)
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_inventory_daily'::regclass
                   AND conname='ck_inventory_supply_valid') THEN
        ALTER TABLE public.Fact_Inventory_Daily ADD CONSTRAINT ck_inventory_supply_valid CHECK (
            (lead_time_days IS NULL OR lead_time_days > 0) AND (incoming_stock IS NULL OR incoming_stock >= 0)
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.fact_inventory_daily'::regclass
                   AND conname='ck_inventory_central_scope') THEN
        ALTER TABLE public.Fact_Inventory_Daily ADD CONSTRAINT ck_inventory_central_scope
            CHECK (shop_key IS NULL);
    END IF;
END $$;

-- Nhãn ngày/tháng: TO_CHAR 'Day'/'Month' cũ đệm khoảng trắng tới 9 ký tự ('Monday   ').
-- Chỉ sửa nhãn hiển thị của dimension; không đổi date_key, full_date hay fact. Chạy lại không đổi gì.
UPDATE public.Dim_Dates
SET day_name = btrim(day_name), month_name = btrim(month_name)
WHERE day_name <> btrim(day_name) OR month_name <> btrim(month_name);

COMMENT ON TABLE public.Fact_Orders IS
    'Grain: one latest source line per (platform, order_id, source_line_id). Not an order header or event log.';
COMMENT ON TABLE public.Fact_Inventory_Daily IS
    'Grain: latest central-warehouse snapshot per (product_key, date_key). No per-shop stock allocation.';
COMMENT ON COLUMN public.Fact_Inventory_Daily.snapshot_at IS
    'Source event timestamp. NULL on legacy snapshots means unknown; loaded_at is not a substitute.';
COMMENT ON COLUMN public.Dim_Products.unit_cost IS
    'Legacy name: ETL writes original selling price here. Do not use as procurement cost or profit input.';
COMMENT ON COLUMN public.Dim_Dates.day_of_week IS
    'PostgreSQL DOW: Sunday=0, Monday=1, ..., Saturday=6.';
