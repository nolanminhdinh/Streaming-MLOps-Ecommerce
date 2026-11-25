// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT g.geo_key, g.state, g.region,#(lf)               COALESCE(SUM(f.buyer_total_amount), 0) AS total_revenue,#(lf)               COALESCE(SUM(f.quantity), 0) AS total_units,#(lf)               COUNT(DISTINCT (f.platform, f.order_id))#(lf)                   FILTER (WHERE f.order_fact_id IS NOT NULL) AS total_orders#(lf)        FROM Dim_Geography g#(lf)        LEFT JOIN Fact_Orders f#(lf)               ON f.geo_key = g.geo_key AND f.is_cancelled = FALSE#(lf)        GROUP BY g.geo_key, g.state, g.region#(lf)        HAVING COUNT(f.order_fact_id) > 0#(lf)        ORDER BY total_revenue DESC"
    ])
in
    Source
