"""Validate all Import SELECTs in one read-only PostgreSQL snapshot; no PBIX refresh."""
import json
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from powerbi.export_powerbi_dataset import DATASETS, DATASET_QUERIES, _database_url


def main():
    load_dotenv(ROOT / '.env')
    engine = create_engine(_database_url(), pool_pre_ping=True, connect_args={'connect_timeout': 5})
    checked_at = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
    result = {'status': 'FAIL', 'checked_at': checked_at.isoformat(),
              'source': 'postgresql_import', 'transaction_read_only': True,
              'powerbi_desktop_refresh_verified': False, 'datasets': {}}
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as conn:
            with conn.begin():
                conn.exec_driver_sql('SET TRANSACTION READ ONLY')
                for name, sql in DATASET_QUERIES.items():
                    columns = list(conn.execute(text(f'SELECT * FROM ({sql}) dataset LIMIT 0')).keys())
                    if columns != list(DATASETS[name]):
                        raise RuntimeError(f'Import column mismatch: {name}')
                    count = conn.execute(text(f'SELECT COUNT(*) FROM ({sql}) dataset')).scalar_one()
                    result['datasets'][Path(name).stem] = {'columns_match': True, 'rows': count}
                summary = conn.execute(text('''SELECT SUM(total_orders) AS orders,
                    SUM(total_units_sold) AS units, SUM(total_revenue) AS revenue
                    FROM vw_powerbi_executive_summary''')).mappings().one()
                facts = conn.execute(text('''SELECT COUNT(DISTINCT (f.platform,f.order_id)) AS orders,
                    SUM(f.quantity) AS units, SUM(f.buyer_total_amount) AS revenue
                    FROM Fact_Orders f JOIN Dim_Dates d ON d.date_key=f.date_key
                    WHERE f.is_cancelled=FALSE''')).mappings().one()
                if (summary['orders'] or 0, summary['units'] or 0, summary['revenue'] or 0) != (
                    facts['orders'] or 0, facts['units'] or 0, facts['revenue'] or 0):
                    raise RuntimeError('Import summary totals differ from facts')
                result.update(status='PASS', summary_matches_warehouse=True,
                              summary_totals=dict(summary))
    except Exception as exc:
        result['error_type'] = type(exc).__name__
        raise
    finally:
        engine.dispose()
        path = ROOT / 'docs/07-bao-cao-do-an/evidence' / f'powerbi_postgres_import_{checked_at:%Y-%m-%d}.json'
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
