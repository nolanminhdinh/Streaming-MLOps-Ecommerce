"""Validate/apply additive integrity constraints and the BI schema atomically.

Default is rollback-only rehearsal. --apply commits after conservation checks.
"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from powerbi.export_powerbi_dataset import _database_url, _install_views
from warehouse.reporting.install import install_bi_schema


def baseline(conn):
    return dict(conn.execute(text('''SELECT
        (SELECT COUNT(*) FROM public.fact_orders) AS order_lines,
        (SELECT COALESCE(SUM(quantity),0) FROM public.fact_orders) AS units,
        (SELECT COALESCE(SUM(buyer_total_amount),0) FROM public.fact_orders) AS buyer_amount,
        (SELECT COUNT(*) FROM public.fact_inventory_daily) AS inventory_snapshots,
        (SELECT COALESCE(SUM(stock_on_hand),0) FROM public.fact_inventory_daily) AS stock,
        (SELECT COUNT(*) FROM public.fact_forecast_predictions) AS forecast_issues,
        (SELECT COUNT(*) FROM public.fact_historical_order_lines) AS historical_lines
    ''')).mappings().one())


def apply_changes(conn):
    conn.exec_driver_sql((ROOT / 'warehouse/ddl/04_integrity_constraints.sql').read_text(encoding='utf-8'))
    _install_views(conn)
    install_bi_schema(conn)


def validate_reporting(conn):
    checks = {}
    comparisons = {
        'order_lines': ('public.fact_orders', 'bi.fact_order_line'),
        'daily_inventory': ('public.fact_inventory_daily', 'bi.fact_inventory_daily'),
        'forecasts': ('public.fact_forecast_predictions', 'bi.fact_forecast_issue'),
        'historical_lines': ('public.fact_historical_order_lines', 'bi.fact_historical_order_line'),
    }
    for name, (source, view) in comparisons.items():
        count = conn.execute(text(f'SELECT (SELECT COUNT(*) FROM {source})=(SELECT COUNT(*) FROM {view})')).scalar_one()
        if not count:
            raise RuntimeError(f'Reporting grain mismatch: {name}')
        checks[name] = True
    checks['historical_keys_complete'] = conn.execute(text('''SELECT NOT EXISTS (
        SELECT 1 FROM bi.fact_historical_order_line f LEFT JOIN bi.dim_historical_product d
        USING(historical_product_key) WHERE d.historical_product_key IS NULL)''')).scalar_one()
    checks['inventory_policy_does_not_drop_skus'] = conn.execute(text('''SELECT
        (SELECT COUNT(*) FROM bi.inventory_latest)=(SELECT COUNT(*) FROM bi.inventory_health)''')).scalar_one()
    if not all(checks.values()):
        raise RuntimeError('BI relationship validation failed')
    checks['order_header_conflicts'] = conn.execute(text('SELECT COUNT(*) FROM bi.quality_order_header_conflicts')).scalar_one()
    checks['legacy_inventory_unknown_time'] = conn.execute(text('SELECT COUNT(*) FROM bi.fact_inventory_daily WHERE NOT snapshot_time_known')).scalar_one()
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Commit changes after validation; default rolls back')
    args = parser.parse_args()
    load_dotenv(ROOT / '.env')
    engine = create_engine(_database_url(), connect_args={'connect_timeout': 5})
    result = {'mode': 'apply' if args.apply else 'rehearsal_rollback', 'status': 'FAILED'}
    now = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
    result['checked_at'] = now.isoformat()
    try:
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                conn.exec_driver_sql("SET LOCAL lock_timeout='5s'")
                conn.exec_driver_sql("SET LOCAL statement_timeout='120s'")
                if not conn.execute(text('SELECT pg_try_advisory_xact_lock(20261101,1)')).scalar_one():
                    raise RuntimeError('Another warehouse migration is running')
                conn.exec_driver_sql('''LOCK TABLE public.fact_orders,public.fact_inventory_daily,
                    public.fact_forecast_predictions,public.fact_historical_order_lines IN SHARE ROW EXCLUSIVE MODE''')
                result['before'] = baseline(conn)
                apply_changes(conn)
                result['validation'] = validate_reporting(conn)
                result['after'] = baseline(conn)
                if result['before'] != result['after']:
                    raise RuntimeError('Migration changed fact totals')
                if args.apply:
                    transaction.commit()
                else:
                    transaction.rollback()
                result['status'] = 'COMMITTED' if args.apply else 'REHEARSAL_PASSED_ROLLED_BACK'
            except Exception:
                transaction.rollback()
                raise
    except Exception as exc:
        result['error_type'] = type(exc).__name__
        raise
    finally:
        engine.dispose()
        suffix = 'apply' if args.apply else 'rehearsal'
        path = ROOT / f'docs/07-bao-cao-do-an/evidence/warehouse_schema_{suffix}_{now:%Y-%m-%d}.json'
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=True, default=str))


if __name__ == '__main__':
    main()
