// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT d.date_key, s.order_date, s.platform, s.total_orders,#(lf)               s.total_units_sold AS units_sold, s.gross_revenue, s.discounts,#(lf)               s.total_revenue AS net_revenue, s.average_order_value AS aov#(lf)        FROM vw_powerbi_executive_summary s#(lf)        JOIN Dim_Dates d ON d.full_date = s.order_date#(lf)        ORDER BY s.order_date, s.platform"
    ])
in
    Source
