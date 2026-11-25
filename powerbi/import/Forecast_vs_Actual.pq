// PostgreSQL source. Select Import in Power BI Desktop.
let
    Source = PostgreSQL.Database(pServer, pDatabase, [
        CreateNavigationProperties = false,
        CommandTimeout = #duration(0, 0, 10, 0),
        Query = "SELECT date_key, product_key, date, sku, product_name, category, matrix_segment,#(lf)               actual_demand, forecast_demand, lower_bound, upper_bound,#(lf)               absolute_error, squared_error, model_name, model_version,#(lf)               model_source, forecast_generated_at, data_origin, source_datasets, actual_complete#(lf)        FROM vw_powerbi_forecast_vs_actual#(lf)        ORDER BY date, sku"
    ])
in
    Source
