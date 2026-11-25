import collections, concurrent.futures, datetime as dt, json, os, pathlib, time, urllib.request, uuid
from kafka import KafkaProducer
from sqlalchemy import text
from warehouse.etl.load import get_engine
root=pathlib.Path('/workspace/data/demo')
run='SOAK'+uuid.uuid4().hex[:10]
path=root/(run+'.json')
duration=600
api='http://fastapi:8000'
fixture=json.load(open('/workspace/output/e2e-2026-11-04/api-monitor.json'))['forecast']['data']
forecast={k:fixture[k] for k in ('sku','from_date','to_date')}
def get(endpoint, body=None):
    req=urllib.request.Request(api+endpoint,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=12) as res:
        return json.load(res)
before=get('/ready')
assert before['model_loaded'] and before['warehouse_connected']
p=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',retries=3)
engine=get_engine()
start=time.monotonic()
deadline=start+duration
ack=[]
errors=[]
samples=[]
def writer():
    for i in range(duration):
        time.sleep(max(0,start+i-time.monotonic()))
        if time.monotonic()>=deadline: break
        ts=dt.datetime.now(dt.timezone.utc).isoformat()
        oid=f'{run}-{i:04d}'
        ev=dict(_platform='shopee',order_sn=oid,item_sku=run,item_name='Soak fixture',shop_id=run,shop_name='Soak audit',quantity=1,original_price=10000,total_order_value=10000,buyer_total_amount=10000,order_status='UNPAID',create_time=ts,update_time=ts)
        try:
            p.send('ecom.orders.raw',key=oid.encode(),value=json.dumps(ev).encode()).get(timeout=10)
            ack.append(oid)
            if i%30==0:
                p.send('inventory.logs',key=run.encode(),value=json.dumps(dict(sku=run,stock_on_hand=50+i,loaded_at=ts)).encode()).get(timeout=10)
        except Exception as ex: errors.append((i,type(ex).__name__))
def client(slot):
    measured=[]
    for i in range(duration):
        time.sleep(max(0,start+i+slot/4-time.monotonic()))
        t=time.monotonic()
        if t>=deadline: break
        try:
            if i%4==0:
                r=get('/inventory/reorder-alert',{'skus':[forecast['sku']],'current_stocks':{forecast['sku']:50}})
                status='OK' if len(r['alerts'])==1 else 'BAD_INVENTORY'
            else:
                r=get('/predict/demand',forecast)
                status='OK' if r['model_source']=='mlflow_registry' and r['model_version']==fixture['model_version'] else 'FALLBACK_OR_VERSION'
        except Exception as ex: status=type(ex).__name__
        measured.append((round(t-start,3),round((time.monotonic()-t)*1000,3),status,'inventory' if i%4==0 else 'forecast'))
    return measured
def warehouse():
    with engine.connect() as c:
        facts=c.execute(text('SELECT order_id,count(*) FROM fact_orders WHERE order_id LIKE :p GROUP BY order_id'),{'p':run+'-%'}).all()
        stock=c.execute(text('SELECT stock_on_hand FROM fact_inventory_daily JOIN dim_products USING(product_key) WHERE sku=:sku ORDER BY snapshot_at DESC LIMIT 1'),{'sku':run}).scalar()
    return dict(facts),stock
def sample():
    rows,stock=warehouse()
    state=json.load(open('/workspace/data/pipeline/status.json'))
    s={'elapsed_seconds':round(time.monotonic()-start,1),'ack':len(ack),'fact_rows':sum(rows.values()),'stock':stock,'worker':state['status'],'pending_objects_at_last_scan':state.get('pending_objects')}
    samples.append(s)
    print(json.dumps(s),flush=True)
    path.write_text(json.dumps({'status':'RUNNING','run_id':run,'samples':samples},indent=2))
with concurrent.futures.ThreadPoolExecutor(5) as pool:
    futures=[pool.submit(client,i) for i in range(4)]
    send=pool.submit(writer)
    for offset in range(30,duration+1,30):
        time.sleep(max(0,start+offset-time.monotonic()))
        try: sample()
        except Exception as ex: print(type(ex).__name__,flush=True)
    send.result()
    records=[r for f in futures for r in f.result()]
p.close(timeout=5)
drain=time.monotonic()
while True:
    rows,stock=warehouse()
    if (set(rows)==set(ack) and stock==620) or time.monotonic()-drain>120: break
    time.sleep(2)
def stats(rr):
    lat=sorted(r[1] for r in rr)
    return {'requests':len(rr),'status':dict(collections.Counter(r[2] for r in rr)),'p50_ms':lat[int(.5*(len(lat)-1))],'p95_ms':lat[int(.95*(len(lat)-1))],'p99_ms':lat[int(.99*(len(lat)-1))],'max_ms':lat[-1]}
out={'run_id':run,'duration_seconds':duration,'target_orders_per_second':1,'target_api_rps':4,'ack_orders':len(ack),'producer_errors':errors,'fact_rows':sum(rows.values()),'missing_orders':sorted(set(ack)-set(rows)),'extra_orders':sorted(set(rows)-set(ack)),'duplicate_fact_rows':sum(v-1 for v in rows.values()),'expected_latest_stock':620,'latest_stock':stock,'drain_seconds':round(time.monotonic()-drain,2),'http':stats(records),'forecast':stats([r for r in records if r[3]=='forecast']),'inventory':stats([r for r in records if r[3]=='inventory']),'samples':samples,'ready_after':get('/ready')}
out['status']='PASS' if len(ack)==duration and not errors and set(rows)==set(ack) and all(v==1 for v in rows.values()) and stock==620 and len(records)>=duration*4*.98 and all(r[2]=='OK' for r in records) else 'FAIL'
path.write_text(json.dumps(out,indent=2))
print('RESULT '+json.dumps(out),flush=True)
