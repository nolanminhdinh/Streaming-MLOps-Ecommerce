// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT d.date_key, h.create_time::DATE AS order_date, h.sku,#(lf)               MAX(h.product_name) AS product_name, h.platform, SUM(h.quantity) AS units_sold,#(lf)               h.data_origin, h.source_dataset#(lf)        FROM Fact_Historical_Order_Lines h#(lf)        JOIN Dim_Dates d ON d.full_date = h.create_time::DATE#(lf)        WHERE h.is_cancelled = FALSE#(lf)        GROUP BY d.date_key, h.create_time::DATE, h.sku, h.platform, h.data_origin, h.source_dataset#(lf)        ORDER BY order_date, h.sku"
    ])
in
    Source
