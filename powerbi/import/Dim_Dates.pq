// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT date_key, full_date, day_of_week, day_name, month, month_name,#(lf)               quarter, year, is_weekend, is_mega_sale#(lf)        FROM Dim_Dates#(lf)        ORDER BY full_date"
    ])
in
    Source
