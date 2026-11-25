// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT p.product_key, p.sku, p.product_name, p.category,#(lf)               COALESCE(m.avg_daily_demand, 0) AS base_demand,#(lf)               COALESCE(m.std_daily_demand, 0) AS sigma,#(lf)               COALESCE(m.matrix_segment, 'NA') AS matrix_segment#(lf)        FROM Dim_Products p#(lf)        LEFT JOIN vw_powerbi_abc_xyz_matrix m ON m.sku = p.sku#(lf)        ORDER BY p.product_key"
    ])
in
    Source
