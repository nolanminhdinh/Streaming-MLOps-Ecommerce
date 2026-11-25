"""Real PostgreSQL acceptance in rollback-only schemas, with exported data."""
from contextlib import contextmanager
from datetime import date,datetime,timezone,timedelta
import os
from pathlib import Path
import uuid
import pandas as pd
import pytest
from sqlalchemy import create_engine,text
from dotenv import load_dotenv
from tests.test_order_line_integrity import event
from warehouse.etl import pipeline,load as loader
from warehouse.etl.transform import unify_schema,clean_data
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.coordination import pipeline_lock,PipelineBusy
from powerbi.export_powerbi_dataset import _database_url
from scripts.backfill_pipeline import run_backfill,snapshot

ROOT=Path(__file__).resolve().parents[1]
pytestmark=pytest.mark.skipif(os.getenv("RUN_PIPELINE_COMPLETION_PG")!="1",reason="Opt-in real PostgreSQL rollback-only acceptance")


class ConnectionEngine:
    def __init__(self,conn):self.conn=conn
    @contextmanager
    def begin(self):
        with self.conn.begin_nested():yield self.conn
    @contextmanager
    def connect(self):yield self.conn


@pytest.fixture
def isolated_warehouse(case_data):
    load_dotenv(ROOT/".env")
    engine=create_engine(_database_url())
    name="pipeline_acceptance_"+uuid.uuid4().hex[:12]
    with engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.exec_driver_sql(f"CREATE SCHEMA {name}")
            conn.exec_driver_sql(f"SET LOCAL search_path TO {name}")
            conn.exec_driver_sql((ROOT/"warehouse/ddl/01_star_schema.sql").read_text(encoding="utf-8"))
            case_data("database-isolation",inputs={"schema":name,"public_facts_written":False},expected="rollback-only schema",actual={"schema":conn.execute(text("SELECT current_schema()")).scalar_one()})
            yield ConnectionEngine(conn),conn,engine
        finally:
            tx.rollback()
    with engine.connect() as conn:
        remains=conn.execute(text("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=:name"),{"name":name}).scalar_one()
    case_data("database-cleanup",inputs={"schema":name},expected={"schemas_remaining":0},actual={"schemas_remaining":remains})
    engine.dispose()
    assert remains==0


def normalized(rows):
    valid,bad,_=DataValidator().validate(clean_data(unify_schema(pd.DataFrame(rows)),preserve_invalid=True))
    assert bad.empty
    return valid


def test_real_backfill_dry_run_apply_replay_and_old_version_guard(isolated_warehouse,monkeypatch,case_data):
    facade,conn,_=isolated_warehouse
    monkeypatch.setattr(loader,"get_engine",lambda:facade)
    newest=event(order_sn="BACKFILL-A",quantity=7,total_order_value=70,buyer_total_amount=70,
                 update_time="2026-10-14T03:00:00Z")
    loader.load(normalized([newest]),engine=facade)
    old=event(order_sn="BACKFILL-A",quantity=2,update_time="2026-10-14T01:00:00Z")
    new=event(order_sn="BACKFILL-B",item_sku="B",quantity=3,total_order_value=30,buyer_total_amount=30)
    invalid=event(order_sn="BACKFILL-C",quantity={"breaking":"type"})
    rows=[old,new,new,invalid]
    received=datetime.now(timezone.utc)-timedelta(seconds=2)
    raw=pd.DataFrame([{**row,"_source_object":"acceptance/backfill.parquet","_source_row":i,
                       "_pipeline_ingested_at":received.isoformat()} for i,row in enumerate(rows)])
    class Lake:
        def __init__(self):self.quarantine=[]
        def put_object(self,**kwargs):self.quarantine.append(kwargs["data"].read().decode())
    lake=Lake()
    monkeypatch.setattr(pipeline,"build_minio_client",lambda:lake)
    monkeypatch.setattr(pipeline,"extract_orders",lambda *args,**kwargs:raw.copy())
    snap=lambda start,end:snapshot(facade,start,end)
    start=date(2026,10,14)
    dry=run_backfill(start,start,etl=pipeline.run_etl_pipeline,snapshot_fn=snap)
    assert dry["business_output_unchanged"] is True and not lake.quarantine
    first=run_backfill(start,start,apply=True,etl=pipeline.run_etl_pipeline,snapshot_fn=snap)
    second=run_backfill(start,start,apply=True,etl=pipeline.run_etl_pipeline,snapshot_fn=snap)
    output=[dict(r) for r in conn.execute(text("SELECT order_id,source_line_id,quantity,order_status,source_updated_at FROM Fact_Orders ORDER BY order_id")).mappings()]
    case_data("raw-events",inputs=rows,expected={"A":7,"B":3,"C":"quarantined","lines":2},actual=output)
    case_data("backfill-before-after",inputs={"date":start},expected={"dry_run_unchanged":True,"second_apply_unchanged":True},actual={"dry_run":dry,"first_apply":first,"second_apply":second})
    case_data("quarantine",inputs=[invalid],expected={"contains":"CONTRACT_INVALID_TYPE_QUANTITY"},actual={"jsonl":lake.quarantine})
    assert [r["quantity"] for r in output]==[7,3]
    assert second["business_output_unchanged"] is True
    assert first["results"][0]["raw_records"]==4
    assert first["results"][0]["duplicates_removed"]==1
    assert first["results"][0]["quarantined_records"]==1
    assert "CONTRACT_INVALID_TYPE_QUANTITY" in lake.quarantine[0]
    ledger=first["results"][0]["object_accounting"][0]
    assert ledger["raw"]==ledger["valid"]+ledger["duplicates"]+ledger["rejected"]
    assert first["results"][0]["ingest_to_warehouse_seconds"]["measured_rows"]==4


def test_real_postgres_lock_excludes_overlapping_jobs(isolated_warehouse,case_data):
    _,_,engine=isolated_warehouse
    observations=[]
    with pipeline_lock(engine):
        with pytest.raises(PipelineBusy):
            with pipeline_lock(engine):observations.append("unexpected_overlap")
        observations.append("second_job_blocked")
    with pipeline_lock(engine):observations.append("next_job_acquired_after_release")
    case_data("real-session-lock",inputs={"concurrent_jobs":2},expected=["second_job_blocked","next_job_acquired_after_release"],actual=observations)
    assert observations==["second_job_blocked","next_job_acquired_after_release"]
