import collections, json, pathlib, sys, time
from kafka import KafkaConsumer, TopicPartition
from warehouse.etl.extract import build_minio_client, list_parquet_files, read_parquet_from_minio, MINIO_BUCKET
from datetime import datetime
root=pathlib.Path('/workspace')
result=json.load(open(root/'output/e2e-2026-11-04/soak.json',encoding='utf-8-sig'))
run=result['run_id']
def inspect_kafka(topic, inventory=False):
    consumer=KafkaConsumer(bootstrap_servers='kafka:9092',enable_auto_commit=False,consumer_timeout_ms=3500)
    partitions=[TopicPartition(topic,n) for n in sorted(consumer.partitions_for_topic(topic))]
    consumer.assign(partitions)
    start=consumer.beginning_offsets(partitions)
    end=consumer.end_offsets(partitions)
    for t in partitions: consumer.seek(t,max(start[t],end[t]-1300))
    matched=[]
    until=time.monotonic()+35
    try:
        while time.monotonic()<until:
            messages=consumer.poll(timeout_ms=1000,max_records=1000)
            for t, batch in messages.items():
                for message in batch:
                    if message.offset<end[t] and message.key and message.key.decode(errors='replace').startswith(run):
                        matched.append(json.loads(message.value))
            if all(consumer.position(p)>=end[p] for p in partitions):
                break
    finally: consumer.close()
    return matched
orders=inspect_kafka('ecom.orders.raw')
inventory=inspect_kafka('inventory.logs',True)
expected={e['order_sn'] for e in orders}
client=build_minio_client()
objects={}
observed=[]
day=datetime.now().astimezone()
for path in list_parquet_files(client,target_date=day):
    if '/platform=shopee/' not in path:continue
    frame=read_parquet_from_minio(client,MINIO_BUCKET,path)
    if 'order_sn' not in frame:continue
    values=[x for x in frame['order_sn'].dropna().astype(str) if x.startswith(run)]
    if values: objects[path]=len(values);observed.extend(values)
check=set(json.load(open(root/'data/pipeline/checkpoint.json'))['completed_objects'])
summary={'run_id':run,'kafka_orders_readback':len(orders),'kafka_unique_orders':len(expected),'kafka_inventory_readback':len(inventory),'raw_rows':len(observed),'raw_distinct_orders':len(set(observed)),'minio_objects':len(objects),'checkpointed_objects':len(set(objects)&check),'missing_raw_orders':sorted(expected-set(observed)),'fact_rows':result['fact_rows'],'last_inventory_stock':max((i['stock_on_hand'] for i in inventory),default=None)}
summary['status']='PASS' if len(orders)==len(expected)==result['ack_orders']==len(observed)==len(set(observed))==result['fact_rows'] and len(inventory)==20 and summary['last_inventory_stock']==620 and not summary['missing_raw_orders'] and len(objects)==summary['checkpointed_objects'] else 'FAIL'
print(json.dumps(summary),flush=True)
sys.exit(summary['status']!='PASS')

