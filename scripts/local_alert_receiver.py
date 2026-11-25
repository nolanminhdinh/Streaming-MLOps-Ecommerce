"""Local-only alert delivery journal; no messages are sent to other people."""
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
import uuid

LOCK=threading.Lock()
EVENTS=Path(os.getenv("ALERT_EVENTS_PATH","data/monitoring/alerts.jsonl"))


def append_event(payload):
    if not isinstance(payload,dict) or not isinstance(payload.get("alerts"),list):
        raise ValueError("Expected an Alertmanager webhook with an alerts list")
    event={"received_at":datetime.now(timezone.utc).isoformat(),"event_id":uuid.uuid4().hex,"payload":payload}
    line=json.dumps(event,ensure_ascii=False,allow_nan=False)+"\n"
    with LOCK:
        EVENTS.parent.mkdir(parents=True,exist_ok=True)
        if EVENTS.exists() and EVENTS.stat().st_size>10_000_000:
            EVENTS.rename(EVENTS.with_name("alerts-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")+"-"+uuid.uuid4().hex[:6]+".jsonl"))
        with EVENTS.open("a",encoding="utf-8") as output:output.write(line)
    return event


class Handler(BaseHTTPRequestHandler):
    def reply(self,status,value):
        body=json.dumps(value,ensure_ascii=False).encode()
        self.send_response(status);self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        if self.path=="/health":return self.reply(200,{"status":"ok","external_delivery":False})
        if self.path=="/events":
            with LOCK:
                rows=EVENTS.read_text(encoding="utf-8").splitlines()[-100:] if EVENTS.exists() else []
            return self.reply(200,{"events":[json.loads(row) for row in rows]})
        self.reply(404,{"error":"not_found"})
    def do_POST(self):
        if self.path!="/alerts":return self.reply(404,{"error":"not_found"})
        try:
            length=int(self.headers.get("Content-Length","0"))
            if not 0<length<=262144:return self.reply(413,{"error":"invalid_size"})
            event=append_event(json.loads(self.rfile.read(length)))
            self.reply(202,{"event_id":event["event_id"],"alerts":len(event["payload"]["alerts"])})
        except (ValueError,TypeError):self.reply(400,{"error":"invalid_payload"})


if __name__=="__main__":ThreadingHTTPServer(("0.0.0.0",9094),Handler).serve_forever()
