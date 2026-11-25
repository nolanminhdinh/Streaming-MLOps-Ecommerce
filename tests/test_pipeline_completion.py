from datetime import date,datetime,timedelta,timezone
import json
from types import SimpleNamespace
from unittest import mock
import pandas as pd
import pytest
from tests.test_order_line_integrity import event
from warehouse.etl.transform import unify_schema,clean_data,deduplicate_lines
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.extract import list_parquet_files
from warehouse.etl.observability import latency_summary,batch_volume
from warehouse.etl.coordination import pipeline_lock,PipelineBusy
from serving.app.pipeline_status import read_pipeline_status
from scripts.backfill_pipeline import run_backfill


def validate(rows):
    frame=clean_data(unify_schema(pd.DataFrame(rows)),preserve_invalid=True)
    return DataValidator().validate(frame)


@pytest.mark.parametrize("version",[None,"1","1.0","1.7"])
def test_additive_schema_is_backward_compatible(version,case_data):
    row=event(schema_version=version,new_optional_field={"campaign":"acceptance","nested":[1,2]})
    valid,bad,_=validate([row])
    actual={"valid":len(valid),"rejected":len(bad),"preserved":valid.iloc[0].raw_payload["new_optional_field"] if len(valid) else None}
    case_data("additive-change",inputs=[row],expected={"valid":1,"rejected":0,"preserved":row["new_optional_field"]},actual=actual)
    assert actual=={"valid":1,"rejected":0,"preserved":row["new_optional_field"]}


@pytest.mark.parametrize("primary",[None,"1.0"])
def test_secondary_version_alias_cannot_hide_a_breaking_version(primary,case_data):
    bad=event(order_sn="BAD-ALIAS",_schema_version="2.0")
    if primary is not None:bad["schema_version"]=primary
    rows=[event(order_sn="GOOD-ALIAS",schema_version="1.0"),bad]
    valid,rejected,_=validate(rows)
    actual={"valid":len(valid),"rejected":len(rejected),"errors":rejected.iloc[0].rejection_reasons if len(rejected) else []}
    case_data("version-alias",inputs=rows,expected={"valid":1,"rejected":1,"contains":"CONTRACT_UNSUPPORTED_VERSION"},actual=actual)
    assert len(valid)==len(rejected)==1 and "CONTRACT_UNSUPPORTED_VERSION" in actual["errors"]


@pytest.mark.parametrize("change,reason",[
    ({"item_sku":None},"CONTRACT_MISSING_ITEM_SKU"),
    ({"original_price":None,"renamed_price":10},"CONTRACT_MISSING_ORIGINAL_PRICE"),
    ({"quantity":[1,2]},"CONTRACT_INVALID_TYPE_QUANTITY"),
    ({"order_line_id":[1]},"CONTRACT_INVALID_TYPE_ORDER_LINE_ID"),
    ({"voucher_from_seller":{"bad":1}},"CONTRACT_INVALID_TYPE_VOUCHER_FROM_SELLER"),
    ({"schema_version":"2.0"},"CONTRACT_UNSUPPORTED_VERSION"),
    ({"order_status":True},"CONTRACT_INVALID_TYPE_ORDER_STATUS"),
])
def test_breaking_schema_is_quarantined_without_losing_good_rows(change,reason,case_data):
    rows=[event(order_sn="GOOD"),event(order_sn="BAD",**change)]
    valid,bad,_=validate(rows)
    actual={"valid":len(valid),"rejected":len(bad),"errors":bad.iloc[0].rejection_reasons if len(bad) else []}
    case_data("breaking-change",inputs=rows,expected={"valid":1,"rejected":1,"contains_error":reason},actual=actual)
    assert len(valid)==len(bad)==1 and reason in bad.iloc[0].rejection_reasons


def test_backfill_defaults_to_read_only_validation(case_data):
    calls=[]
    def etl(**kwargs):
        calls.append(kwargs);return {"status":"DRY_RUN_SUCCESS","raw_records":2}
    result=run_backfill(date(2026,10,14),date(2026,10,15),etl=etl)
    case_data("dry-run",inputs={"start":"2026-10-14","end":"2026-10-15"},expected={"calls":2,"all_dry_run":True,"checkpoint_modified":False},actual={"calls":calls,"result":result})
    assert len(calls)==2 and all(c["dry_run"] for c in calls) and not result["checkpoint_modified"]


@pytest.mark.parametrize("start,end",[(date(2026,10,15),date(2026,10,14)),(date(2026,1,1),date(2026,3,1))])
def test_backfill_rejects_unbounded_or_reversed_interval(start,end,case_data):
    etl=mock.Mock()
    with pytest.raises(ValueError):run_backfill(start,end,etl=etl)
    case_data("range-gate",inputs={"start":start,"end":end},expected="rejected before any ETL",actual={"etl_calls":etl.call_count})
    etl.assert_not_called()


def test_backfill_stops_at_first_unsuccessful_day(case_data):
    etl=mock.Mock(return_value={"status":"LOAD_ERROR"})
    with pytest.raises(RuntimeError) as failure:run_backfill(date(2026,10,14),date(2026,10,16),apply=True,etl=etl)
    case_data("fail-fast",inputs={"days":3},expected={"etl_calls":1,"failed_date":"2026-10-14"},actual={"etl_calls":etl.call_count,"audit":failure.value.audit})
    assert etl.call_count==1


def test_selected_object_backfill_never_expands_to_other_partitions(case_data):
    name="raw/orders/platform=shopee/year=2026/month=10/day=14/acceptance.parquet"
    etl=mock.Mock(return_value={"status":"DRY_RUN_SUCCESS"})
    result=run_backfill(date(2026,10,14),date(2026,10,15),etl=etl,object_names=[name])
    calls=[c.kwargs for c in etl.call_args_list]
    case_data("selected-objects",inputs=[name],expected={"day1":[name],"day2":[]},actual={"calls":calls,"result":result})
    assert calls[0]["object_names"]==[name] and calls[1]["object_names"]==[]


def test_out_of_range_selected_object_is_rejected_before_io(case_data):
    name="raw/orders/platform=shopee/year=2026/month=10/day=13/acceptance.parquet"
    etl=mock.Mock()
    with pytest.raises(ValueError):run_backfill(date(2026,10,14),date(2026,10,14),etl=etl,object_names=[name])
    case_data("selected-object-gate",inputs=[name],expected={"etl_calls":0},actual={"etl_calls":etl.call_count})
    etl.assert_not_called()


def test_latency_uses_measured_ingestion_not_business_date(case_data):
    end=datetime(2026,11,8,tzinfo=timezone.utc)
    rows=[{"_pipeline_ingested_at":(end-timedelta(seconds=s)).isoformat(),"create_time":"2020-01-01"} for s in range(1,6)]
    rows.extend([{"_pipeline_ingested_at":"bad"},{"_pipeline_ingested_at":(end+timedelta(seconds=3)).isoformat()}])
    result=latency_summary(pd.DataFrame(rows),end)
    case_data("measured-latency",inputs=rows,expected={"measured_rows":5,"p95":4.8},actual=result)
    assert result["measured_rows"]==5 and result["p95"]==pytest.approx(4.8)
    assert latency_summary(pd.DataFrame([{"old":1}]),end)["p95"] is None


@pytest.mark.parametrize("checked,expected",[(995,True),(100,False),(1007,False)])
def test_heartbeat_idle_and_future_timestamp_contract(tmp_path,checked,expected,case_data):
    path=tmp_path/"status.json"
    state={"status":"healthy","checked_at":checked,"pending_objects":0,"oldest_pending_seconds":0,"last_success_at":100}
    path.write_text(json.dumps(state))
    actual=read_pipeline_status(path,now=1000)
    case_data("heartbeat",inputs=state,expected={"ready":expected,"unmeasured_latency":None},actual=actual)
    assert actual["ready"]==expected and actual["ingest_to_warehouse_p95_seconds"] is None
    assert actual["last_success_age_seconds"]==900


def test_batch_volume_warms_up_before_signalling_anomaly(case_data):
    warm=batch_volume(1000,[10]*5);spike=batch_volume(1000,[10]*6);normal=batch_volume(10,[10]*6)
    case_data("volume",inputs={"history":[10]*6,"new_batch":1000},expected={"warmup_anomaly":0,"spike_anomaly":1,"normal_anomaly":0},actual={"warmup":warm,"spike":spike,"normal":normal})
    assert warm["anomaly"]==0 and spike["anomaly"]==1 and normal["anomaly"]==0


def test_object_metadata_supports_queue_age_without_guessing(case_data):
    stamp=datetime(2026,11,8,tzinfo=timezone.utc)
    client=mock.Mock();client.list_objects.return_value=[SimpleNamespace(object_name="raw/a.parquet",last_modified=stamp,size=123),SimpleNamespace(object_name="raw/other.txt")]
    metadata={};names=list_parquet_files(client,metadata=metadata)
    case_data("queue-metadata",inputs={"objects":["raw/a.parquet","raw/other.txt"]},expected={"selected":1,"size":123},actual={"names":names,"metadata":metadata})
    assert names==["raw/a.parquet"] and metadata[names[0]]["last_modified"]==stamp.timestamp()


def test_busy_pipeline_does_not_enter_critical_section(case_data):
    engine=mock.MagicMock();connection=engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one.return_value=False
    entered=False
    with pytest.raises(PipelineBusy):
        with pipeline_lock(engine):entered=True
    case_data("advisory-lock",inputs={"lock_available":False},expected={"entered":False},actual={"entered":entered})
    assert not entered


def test_local_alert_delivery_journals_payload(tmp_path,monkeypatch,case_data):
    from scripts import local_alert_receiver as receiver
    path=tmp_path/"alerts.jsonl";monkeypatch.setattr(receiver,"EVENTS",path)
    payload={"status":"firing","alerts":[{"labels":{"alertname":"AcceptanceOnly","source":"unit-test"}}]}
    event=receiver.append_event(payload)
    saved=json.loads(path.read_text())
    case_data("alert-journal",inputs=payload,expected={"same_payload":True,"external_delivery":False},actual=saved)
    assert saved==event and saved["payload"]==payload


@pytest.mark.parametrize("payload",[{},[],{"alerts":"bad"}])
def test_invalid_alert_does_not_create_journal(payload,tmp_path,monkeypatch,case_data):
    from scripts import local_alert_receiver as receiver
    path=tmp_path/"alerts.jsonl";monkeypatch.setattr(receiver,"EVENTS",path)
    with pytest.raises(ValueError):receiver.append_event(payload)
    case_data("invalid-alert",inputs=payload,expected={"journal_created":False},actual={"journal_created":path.exists()})
    assert not path.exists()
