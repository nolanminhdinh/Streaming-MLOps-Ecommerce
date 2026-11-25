"""Read-only validation of the deployed BI contract and bounded query plans."""
import json
from pathlib import Path
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from dotenv import load_dotenv
from sqlalchemy import create_engine,text
from powerbi.export_powerbi_dataset import _database_url
from scripts.migrate_warehouse_schema import validate_reporting


def main():
    load_dotenv(ROOT / '.env')
    engine=create_engine(_database_url(),connect_args={'connect_timeout':5})
    result={'mode':'read_only_repeatable_read','checked_at':datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).isoformat()}
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as conn:
            with conn.begin():
                conn.exec_driver_sql('SET TRANSACTION READ ONLY')
                conn.exec_driver_sql("SET LOCAL statement_timeout='60s'")
                result['validation']=validate_reporting(conn)
                names=conn.execute(text("SELECT table_name FROM information_schema.views WHERE table_schema='bi' ORDER BY table_name")).scalars().all()
                result['views']={name:conn.execute(text(f'SELECT COUNT(*) FROM bi.{name}')).scalar_one() for name in names}
                result['quality']=dict(conn.execute(text('''SELECT COUNT(*) AS lines,
                    COUNT(*) FILTER(WHERE header_conflict) AS flagged_lines,
                    COUNT(*) FILTER(WHERE missing_shop) AS missing_shop,
                    COUNT(*) FILTER(WHERE missing_geography) AS missing_geography
                    FROM bi.fact_order_line''')).mappings().one())
                result['plans']={}
                queries={
                    'sales_by_category': '''SELECT p.category,SUM(f.quantity) FROM bi.fact_order_line f
                        JOIN bi.dim_product p USING(product_key) WHERE NOT f.is_cancelled GROUP BY p.category''',
                    'latest_inventory': 'SELECT COUNT(*),SUM(current_stock) FROM bi.inventory_latest',
                    'order_conflicts': 'SELECT COUNT(*) FROM bi.quality_order_header_conflicts',
                }
                for name,sql in queries.items():
                    plan=conn.execute(text('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql)).scalar_one()[0]
                    result['plans'][name]={'execution_ms':plan['Execution Time'],'planning_ms':plan['Planning Time'],'plan':plan['Plan']}
    finally:
        engine.dispose()
    path=ROOT/'docs/07-bao-cao-do-an/evidence/reporting_schema_checks_2026-11-01.json'
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=True,default=str))


if __name__=='__main__':
    main()
