"""Read-only structural/data audit before warehouse schema changes."""
import json
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from powerbi.export_powerbi_dataset import _database_url

TABLES = ('dim_products', 'dim_shops', 'dim_geography', 'dim_dates', 'dim_payment',
          'dim_carriers', 'fact_orders', 'fact_inventory_daily',
          'fact_forecast_predictions', 'fact_historical_order_lines', 'warehouse_daily_coverage',
          'model_serving_snapshot')

CHECKS = {
    'orders_required_keys_missing': 'SELECT COUNT(*) FROM fact_orders WHERE product_key IS NULL OR date_key IS NULL',
    'orders_optional_keys_missing': '''SELECT COUNT(*) FILTER(WHERE shop_key IS NULL) AS shop,
        COUNT(*) FILTER(WHERE geo_key IS NULL) AS geography,
        COUNT(*) FILTER(WHERE payment_key IS NULL) AS payment,
        COUNT(*) FILTER(WHERE carrier_key IS NULL) AS carrier FROM fact_orders''',
    'orders_invalid_values': '''SELECT COUNT(*) FROM fact_orders WHERE quantity<=0 OR original_price<0
        OR buyer_total_amount<0 OR platform NOT IN ('shopee','tiktok') OR is_cancelled IS NULL''',
    'orders_wrong_date_key': '''SELECT COUNT(*) FROM fact_orders f JOIN dim_dates d USING(date_key)
        WHERE d.full_date<>f.create_time::DATE''',
    'orders_shop_platform_mismatch': '''SELECT COUNT(*) FROM fact_orders f JOIN dim_shops s USING(shop_key)
        WHERE f.platform<>s.platform''',
    'orders_conflicting_header': '''SELECT COUNT(*) FROM (SELECT platform,order_id FROM fact_orders
        GROUP BY platform,order_id HAVING COUNT(DISTINCT date_key)>1 OR COUNT(DISTINCT shop_key)>1
        OR COUNT(DISTINCT geo_key)>1 OR COUNT(DISTINCT is_cancelled)>1) x''',
    'orders_multiple_lines': '''SELECT COUNT(*) FROM (SELECT platform,order_id FROM fact_orders
        GROUP BY platform,order_id HAVING COUNT(*)>1) x''',
    'inventory_invalid_values': '''SELECT COUNT(*) FROM fact_inventory_daily WHERE product_key IS NULL
        OR date_key IS NULL OR stock_on_hand<0 OR safety_stock<0 OR reorder_point<0''',
    'inventory_missing_event_time': 'SELECT COUNT(*) FROM fact_inventory_daily WHERE snapshot_at IS NULL',
    'inventory_noncentral_shop_keys': 'SELECT COUNT(*) FROM fact_inventory_daily WHERE shop_key IS NOT NULL',
    'inventory_missing_from_health_view': '''SELECT COUNT(DISTINCT i.product_key) FROM fact_inventory_daily i
        WHERE NOT EXISTS(SELECT 1 FROM vw_powerbi_inventory_health v WHERE v.product_key=i.product_key)''',
    'geography_nullable_business_keys': 'SELECT COUNT(*) FROM dim_geography WHERE city IS NULL OR district IS NULL',
    'geography_normalized_duplicates': '''SELECT COUNT(*) FROM (SELECT state,COALESCE(city,''),COALESCE(district,'')
        FROM dim_geography GROUP BY state,COALESCE(city,''),COALESCE(district,'') HAVING COUNT(*)>1) x''',
    'forecast_nullable_identity': 'SELECT COUNT(*) FROM fact_forecast_predictions WHERE history_end_date IS NULL',
    'forecast_duplicate_identity': '''SELECT COUNT(*) FROM (SELECT product_key,target_date,model_name,model_version,
        history_end_date,data_origin,source_datasets FROM fact_forecast_predictions GROUP BY product_key,target_date,
        model_name,model_version,history_end_date,data_origin,source_datasets HAVING COUNT(*)>1) x''',
    'historical_sku_too_long_for_product_dimension': 'SELECT COUNT(*) FROM fact_historical_order_lines WHERE LENGTH(sku)>50',
    'historical_unmapped_skus': '''SELECT COUNT(DISTINCT h.sku) FROM fact_historical_order_lines h
        WHERE NOT EXISTS(SELECT 1 FROM dim_products p WHERE p.sku=h.sku)''',
    'forecast_multiple_evaluation_rows': '''SELECT COUNT(*) FROM (SELECT product_key,date FROM vw_powerbi_forecast_vs_actual
        GROUP BY product_key,date HAVING COUNT(*)>1) x''',
    'orders_baseline': '''SELECT COUNT(*) AS lines,COUNT(DISTINCT(platform,order_id)) AS orders,
        SUM(quantity) AS units,SUM(buyer_total_amount) AS buyer_amount FROM fact_orders WHERE is_cancelled=FALSE''',
    'inventory_baseline': 'SELECT COUNT(*) AS snapshots,SUM(stock_on_hand) AS stock FROM fact_inventory_daily',
}


def main():
    load_dotenv(ROOT / '.env')
    engine = create_engine(_database_url(), connect_args={'connect_timeout': 5})
    now = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
    result = {'checked_at': now.isoformat(), 'mode': 'read_only_repeatable_read', 'tables': {}, 'checks': {}}
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as conn:
            with conn.begin():
                conn.exec_driver_sql('SET TRANSACTION READ ONLY')
                inspector = inspect(conn)
                for name in TABLES:
                    result['tables'][name] = {
                        'rows': conn.execute(text(f'SELECT COUNT(*) FROM {name}')).scalar_one(),
                        'columns': inspector.get_columns(name),
                        'pk': inspector.get_pk_constraint(name),
                        'foreign_keys': inspector.get_foreign_keys(name),
                        'unique_constraints': inspector.get_unique_constraints(name),
                        'checks': inspector.get_check_constraints(name),
                    }
                for name, sql in CHECKS.items():
                    try:
                        with conn.begin_nested():
                            rows = conn.execute(text(sql)).mappings().all()
                            result['checks'][name] = [dict(row) for row in rows]
                    except Exception as exc:
                        result['checks'][name] = {'error_type': type(exc).__name__}
    finally:
        engine.dispose()
    path = ROOT / f'docs/07-bao-cao-do-an/evidence/warehouse_schema_audit_{now:%Y-%m-%d}.json'
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps({'table_rows': {k: v['rows'] for k,v in result['tables'].items()},
                      'checks': result['checks'], 'evidence': str(path)}, ensure_ascii=True, default=str))


if __name__ == '__main__':
    main()
