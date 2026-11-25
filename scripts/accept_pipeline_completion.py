"""Bounded labelled Kafka -> raw -> warehouse -> API acceptance with saved data.

Test facts are retained under a unique CMP prefix. No training, Registry writes,
checkpoint resets, coverage certification or business-data deletion is performed.
"""
from datetime import datetime,timedelta,timezone
import argparse
import csv
import io
import json
import logging
import os
from pathlib import Path
import sys
import time
import uuid
from urllib.request import Request,urlopen

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT/".env")
from kafka import KafkaProducer,KafkaConsumer,TopicPartition
import pandas as pd
from sqlalchemy import text
from ingestion.producer import send_event,KAFKA_BOOTSTRAP_SERVERS,TOPIC_ORDERS,TOPIC_INVENTORY
from warehouse.etl.extract import build_minio_client,read_parquet_from_minio,MINIO_BUCKET
from warehouse.etl.load import get_engine
from warehouse.etl.quarantine import json_value


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(json_value(value),ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")


def api(path,payload=None):
    request=Request("http://localhost:8000"+path,data=json.dumps(payload).encode() if payload is not None else None,
                    headers={"Content-Type":"application/json"})
    with urlopen(request,timeout=40) as response:return {"http_status":response.status,"body":json.load(response)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,default=ROOT/"output/pipeline-completion-2026-11-08/live")
    args=parser.parse_args();out=args.output;out.mkdir(parents=True,exist_ok=True)
    logging.disable(logging.CRITICAL)
    prefix="CMP20261108"+uuid.uuid4().hex[:8]
    now=datetime.now(timezone.utc);base={"_platform":"shopee","item_sku":prefix+"-A","item_name":"Pipeline acceptance fixture",
        "shop_id":prefix,"shop_name":"Pipeline acceptance","original_price":10,"order_status":"COMPLETED",
        "create_time":(now-timedelta(seconds=40)).isoformat(),"schema_version":"1.0"}
    newest={**base,"order_sn":prefix+"-A","quantity":7,"total_order_value":70,"buyer_total_amount":70,
            "update_time":(now-timedelta(seconds=5)).isoformat(),"additive_field":{"accepted":True}}
    second={**base,"order_sn":prefix+"-B","item_sku":prefix+"-B","quantity":3,"total_order_value":30,"buyer_total_amount":30}
    older={**newest,"quantity":2,"total_order_value":20,"buyer_total_amount":20,"update_time":(now-timedelta(seconds=20)).isoformat()}
    rows=[newest,second,older,{**newest,"order_sn":prefix+"-BADTYPE","quantity":[1,2]},
          {**newest,"order_sn":prefix+"-BADVERSION","schema_version":"2.0"},
          {**newest,"order_sn":prefix+"-BADKEY","item_sku":None},newest,second]
    inventory=[{"sku":base["item_sku"],"stock_on_hand":50,"loaded_at":(now-timedelta(seconds=2)).isoformat()},
               {"sku":base["item_sku"],"stock_on_hand":5,"loaded_at":(now-timedelta(seconds=25)).isoformat()}]
    result={"run_id":prefix,"status":"RUNNING","expected":{"order_acks":8,"inventory_acks":2,"raw_rows":8,"fact_rows":2,"quantity":10,"stock":50,"rejected_source_events":3},
            "writes":"labelled acceptance rows retained; no model/coverage change"}
    write(out/"protocol.json",result);write(out/"input-orders.json",rows);write(out/"input-inventory.json",inventory)
    engine=get_engine();producer=None;consumer=None
    try:
        consumer=KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,group_id=None,enable_auto_commit=False,
                              value_deserializer=lambda v:json.loads(v.decode()),request_timeout_ms=15000)
        partitions=sorted(consumer.partitions_for_topic(TOPIC_ORDERS) or [])
        if not partitions:raise RuntimeError("Order topic has no partitions")
        assigned=[TopicPartition(TOPIC_ORDERS,p) for p in partitions];consumer.assign(assigned)
        starts=consumer.end_offsets(assigned)
        for partition,offset in starts.items():consumer.seek(partition,offset)
        producer=KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,acks="all",retries=3,value_serializer=lambda v:json.dumps(v).encode())
        acks=[];began=time.monotonic()
        for row in rows:
            metadata=send_event(producer,row,topic=TOPIC_ORDERS)
            acks.append({"order_id":row["order_sn"],"topic":metadata.topic,"partition":metadata.partition,"offset":metadata.offset})
        for row in inventory:send_event(producer,row,topic=TOPIC_INVENTORY)
        write(out/"kafka-acknowledgements.json",acks)
        received=[];deadline=time.monotonic()+30
        while time.monotonic()<deadline and len(received)<8:
            batches=consumer.poll(timeout_ms=500)
            for partition,messages in batches.items():
                received.extend({"partition":partition.partition,"offset":m.offset,"value":m.value} for m in messages if str(m.value.get("order_sn","")).startswith(prefix))
        write(out/"kafka-readback.json",received)
        assert len(received)==8
        observations=[];facts=[]
        for _ in range(90):
            with engine.connect() as conn:
                facts=[dict(r) for r in conn.execute(text('''SELECT f.order_id,f.source_line_id,p.sku,f.quantity,f.order_status,
                    f.source_updated_at,f.buyer_total_amount FROM Fact_Orders f JOIN Dim_Products p USING(product_key)
                    WHERE f.order_id LIKE :prefix ORDER BY f.order_id'''),{"prefix":prefix+"%"}).mappings()]
            status=api("/pipeline/status")
            observations.append({"elapsed_seconds":time.monotonic()-began,"facts":len(facts),"pipeline":status["body"]})
            if len(facts)==2 and status["body"].get("pending_objects")==0 and status["body"].get("ready"):
                break
            time.sleep(2)
        pd.DataFrame(facts).to_csv(out/"warehouse-output.csv",index=False,encoding="utf-8-sig")
        write(out/"warehouse-output.json",facts);write(out/"pipeline-observations.json",observations)
        assert len(facts)==2 and sum(r["quantity"] for r in facts)==10 and facts[0]["quantity"]==7
        lake=build_minio_client();raw=[];objects=[];rejects=[]
        # Only inspect raw object timestamps generated during this labelled test.
        for obj in lake.list_objects(MINIO_BUCKET,prefix="raw/orders/",recursive=True):
            if not obj.object_name.endswith(".parquet") or obj.last_modified<now-timedelta(seconds=3):continue
            frame=read_parquet_from_minio(lake,MINIO_BUCKET,obj.object_name)
            if "order_sn" not in frame:continue
            selected=frame[frame.order_sn.fillna("").astype(str).str.startswith(prefix)]
            if len(selected):
                raw.extend(selected.to_dict("records"));objects.append(obj.object_name)
        write(out/"minio-raw.json",raw)
        pd.DataFrame(raw).to_csv(out/"minio-raw.csv",index=False,encoding="utf-8-sig")
        checkpoint=json.loads((ROOT/"data/pipeline/checkpoint.json").read_text(encoding="utf-8"))
        runs=[]
        for path in (ROOT/"data/pipeline/runs").glob("*.json"):
            entry=json.loads(path.read_text(encoding="utf-8"))
            if set(objects)&{r["source_object"] for r in entry.get("object_accounting",[])}:
                runs.append(entry)
                name=entry.get("quarantine_object")
                if name:
                    response=lake.get_object(MINIO_BUCKET,name)
                    try:entries=[json.loads(line) for line in response.read().decode().splitlines()]
                    finally:response.close();response.release_conn()
                    rejects.extend(row for row in entries if str(row.get("order_id","")).startswith(prefix))
        write(out/"etl-run-lineage.json",runs);write(out/"quarantine-output.json",rejects)
        pd.DataFrame([{k:row.get(k) for k in ("order_id","rejection_reasons","source_object","source_row")} for row in rejects]).to_csv(out/"quarantine-output.csv",index=False,encoding="utf-8-sig")
        assert len(raw)==8 and len(rejects)==3 and set(objects)<=set(checkpoint["completed_objects"])
        inventory_result=api("/inventory/reorder-alert",{"skus":[base["item_sku"]]})
        write(out/"inventory-api.json",inventory_result)
        assert inventory_result["body"]["alerts"][0]["current_stock"]==50
        history=pd.read_parquet(ROOT/"data/daily_history.parquet")
        sku=str(history.sku.iloc[0]);last=pd.to_datetime(history.date).max().date()
        payload={"sku":sku,"from_date":str(last+timedelta(days=1)),"to_date":str(last+timedelta(days=7))}
        forecast=api("/predict/demand",payload)
        write(out/"forecast-api.json",{"input":payload,"output":forecast,"scope":"separate historical-source consumption branch; no accuracy claim"})
        assert forecast["http_status"]==200
        result.update(status="PASS",actual={"kafka_readback":len(received),"raw_rows":len(raw),"raw_objects":len(objects),
            "fact_rows":len(facts),"quantity":sum(r["quantity"] for r in facts),"rejected_events":len(rejects),
            "checkpointed_objects":len(set(objects)&set(checkpoint["completed_objects"])),
            "stock":inventory_result["body"]["alerts"][0]["current_stock"],"observed_completion_seconds":observations[-1]["elapsed_seconds"]})
    except Exception as exc:
        result.update(status="FAIL",error_type=type(exc).__name__,error=str(exc));raise
    finally:
        if producer:producer.close()
        if consumer:consumer.close()
        engine.dispose();write(out/"result.json",result)
        print(json.dumps(result,ensure_ascii=False,default=str))


if __name__=="__main__":main()
