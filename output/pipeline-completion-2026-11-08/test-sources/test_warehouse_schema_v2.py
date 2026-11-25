"""Rollback-only PostgreSQL checks in isolated source/reporting schemas.

No production rows or identity sequences are touched. Opt in with
RUN_WAREHOUSE_SCHEMA_TESTS=1.
"""
import os
from pathlib import Path
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from powerbi.export_powerbi_dataset import _database_url
from scripts.audit_warehouse_schema import TABLES

pytestmark = pytest.mark.skipif(os.getenv('RUN_WAREHOUSE_SCHEMA_TESTS') != '1',
                                reason='Opt-in isolated PostgreSQL schema checks')
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def warehouse():
    load_dotenv(ROOT / '.env')
    engine = create_engine(_database_url())
    suffix = uuid.uuid4().hex[:10]
    source, report = f'audit_source_{suffix}', f'audit_bi_{suffix}'
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.exec_driver_sql(f'CREATE SCHEMA {source}')
            for table in (*TABLES, 'vw_powerbi_inventory_health', 'vw_powerbi_forecast_vs_actual'):
                conn.exec_driver_sql(f'CREATE TABLE {source}.{table} AS SELECT * FROM public.{table} WITH NO DATA')
            conn.exec_driver_sql(f'''INSERT INTO {source}.dim_dates(date_key,full_date) VALUES
                (1,'2026-10-01'),(2,'2026-10-02');
                INSERT INTO {source}.dim_products(product_key,sku,product_name) VALUES (1,'A','A'),(2,'B','B');''')
            hardening = (ROOT / 'warehouse/ddl/04_integrity_constraints.sql').read_text(encoding='utf-8').replace('public.', source + '.')
            reporting = (ROOT / 'warehouse/reporting/bi_schema.sql').read_text(encoding='utf-8')
            reporting = reporting.replace('public.', source + '.').replace('bi.', report + '.')
            reporting = reporting.replace('SCHEMA IF NOT EXISTS bi;', f'SCHEMA IF NOT EXISTS {report};')
            reporting = reporting.replace('SCHEMA bi IS', f'SCHEMA {report} IS')
            conn.exec_driver_sql(hardening)
            conn.exec_driver_sql(reporting)
            yield conn, source, report, hardening, reporting
        finally:
            transaction.rollback()
    engine.dispose()


def insert_line(conn, source, key, sku, order='shared', platform='shopee', geo=1):
    conn.execute(text(f'''INSERT INTO {source}.fact_orders
        (order_fact_id,product_key,date_key,order_id,source_line_id,platform,is_cancelled,quantity,
         original_price,subtotal,buyer_total_amount,geo_key)
        VALUES (:key,:sku,1,:order,:line,:platform,FALSE,2,10,20,20,:geo)'''),
        dict(key=key,sku=sku,order=order,line=f'line-{key}',platform=platform,geo=geo))


def test_order_line_grain_shared_keys_and_distinct_business_order(warehouse):
    conn, source, report, *_ = warehouse
    insert_line(conn, source, 1, 1)
    insert_line(conn, source, 2, 2)
    insert_line(conn, source, 3, 1, platform='tiktok')
    rows = conn.execute(text(f'''SELECT COUNT(*) AS lines,COUNT(DISTINCT order_business_key) AS orders,
        SUM(quantity) AS units,SUM(buyer_amount) AS amount FROM {report}.fact_order_line''')).one()
    assert rows == (3,2,6,60)
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.quality_order_header_conflicts')).scalar_one() == 0


def test_conflicts_are_flagged_without_dropping_or_rewriting_lines(warehouse):
    conn, source, report, *_ = warehouse
    insert_line(conn, source, 1, 1, geo=1)
    insert_line(conn, source, 2, 2, geo=2)
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.fact_order_line WHERE header_conflict')).scalar_one() == 2
    assert conn.execute(text(f'SELECT geography_count FROM {report}.quality_order_header_conflicts')).scalar_one() == 2


def test_latest_stock_does_not_sum_days_or_drop_products_without_policy(warehouse):
    conn, source, report, *_ = warehouse
    conn.exec_driver_sql(f'''INSERT INTO {source}.fact_inventory_daily
        (inventory_fact_id,product_key,date_key,stock_on_hand) VALUES (1,1,1,10),(2,1,2,7),(3,2,1,50)''')
    assert conn.execute(text(f'SELECT SUM(current_stock) FROM {report}.inventory_latest')).scalar_one() == 57
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.inventory_health')).scalar_one() == 2
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.inventory_health WHERE demand_policy_available')).scalar_one() == 0
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.inventory_latest WHERE snapshot_time_known')).scalar_one() == 0


def test_external_catalog_uses_source_and_dataset_not_live_product_key(warehouse):
    conn, source, report, *_ = warehouse
    for key, dataset in [(1,'file-a'),(2,'file-b'),(3,'file-a')]:
        conn.execute(text(f'''INSERT INTO {source}.fact_historical_order_lines
            (historical_line_id,data_origin,source_dataset,sku,quantity,product_name)
            VALUES (:key,'historical',:dataset,'not-in-live-catalog',1,'External item')'''), dict(key=key,dataset=dataset))
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.dim_historical_product')).scalar_one() == 2
    assert conn.execute(text(f'''SELECT COUNT(*) FROM {report}.fact_historical_order_line f
        JOIN {report}.dim_historical_product d USING(historical_product_key)''')).scalar_one() == 3


@pytest.mark.parametrize('column,value', [('quantity','0'),('quantity','-1'),('original_price',"'NaN'::numeric"),
                                        ('buyer_total_amount','-1'),('product_key','NULL'),('date_key','NULL')])
def test_database_rejects_invalid_order_rows(warehouse, column, value):
    conn, source, *_ = warehouse
    insert_line(conn, source, 1, 1)
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            conn.exec_driver_sql(f'UPDATE {source}.fact_orders SET {column}={value}')


def test_database_rejects_negative_and_per_shop_central_stock(warehouse):
    conn, source, *_ = warehouse
    conn.exec_driver_sql(f'''INSERT INTO {source}.fact_inventory_daily
        (inventory_fact_id,product_key,date_key,stock_on_hand) VALUES (1,1,1,10)''')
    for assignment in ['stock_on_hand=-1','shop_key=1','product_key=NULL']:
        with pytest.raises(IntegrityError):
            with conn.begin_nested():
                conn.exec_driver_sql(f'UPDATE {source}.fact_inventory_daily SET {assignment}')


def test_additive_schema_can_be_applied_twice(warehouse):
    conn, source, report, hardening, reporting = warehouse
    insert_line(conn, source, 1, 1)
    conn.exec_driver_sql(hardening)
    conn.exec_driver_sql(reporting)
    assert conn.execute(text(f'SELECT COUNT(*) FROM {report}.fact_order_line')).scalar_one() == 1
