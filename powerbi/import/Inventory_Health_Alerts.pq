// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT date_key, product_key, sku, product_name, category, matrix_segment,#(lf)               current_stock, avg_daily_demand, safety_stock, reorder_point,#(lf)               needs_reorder, alert_level, days_until_stockout, recommended_reorder_qty#(lf)        FROM vw_powerbi_inventory_health#(lf)        ORDER BY CASE alert_level WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,#(lf)                 days_until_stockout"
    ])
in
    Source
