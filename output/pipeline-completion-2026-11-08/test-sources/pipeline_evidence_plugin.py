"""Opt-in per-test evidence. Explicit snapshots avoid dumping secrets from locals."""
from datetime import date,datetime
from decimal import Decimal
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import pytest

RECORDS={}


def safe(value):
    if isinstance(value,float) and not math.isfinite(value):return str(value)
    if isinstance(value,dict):
        return {str(k):"[redacted]" if any(s in str(k).lower() for s in ("password","secret","token")) else safe(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [safe(v) for v in value]
    if isinstance(value,(datetime,date,Decimal)):return str(value)
    if hasattr(value,"to_dict"):return safe(value.to_dict(orient="records"))
    if hasattr(value,"tolist"):return safe(value.tolist())
    if value is None or isinstance(value,(str,int,float,bool)):return value
    return "<"+type(value).__name__+">"


def pytest_addoption(parser):
    parser.addoption("--pipeline-evidence-dir",default=None,help="Write per-test results and explicit input/output snapshots")


def directory(config):
    value=config.getoption("--pipeline-evidence-dir")
    return Path(value).resolve() if value else None


def case_folder(item):
    root=directory(item.config)
    if root is None:return None
    # Stay below Windows MAX_PATH even in the user's long workspace path.
    name=re.sub(r"[^A-Za-z0-9_-]+","-",item.name)[:40]+"-"+hashlib.sha256(item.nodeid.encode()).hexdigest()[:10]
    path=root/"cases"/name;path.mkdir(parents=True,exist_ok=True)
    return path


@pytest.fixture
def case_data(request):
    def record(name,*,inputs=None,expected=None,actual=None):
        payload=safe({"test":request.node.nodeid,"name":name,"inputs":inputs,"expected":expected,"actual":actual})
        folder=case_folder(request.node)
        if folder:
            stem=re.sub(r"[^A-Za-z0-9_-]+","-",name)[:45]
            (folder/(stem+".json")).write_text(json.dumps(payload,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
            for label,rows in (("inputs",inputs),("actual",actual)):
                rows=safe(rows)
                if isinstance(rows,list) and rows and all(isinstance(r,dict) for r in rows):
                    keys=sorted({k for row in rows for k in row})
                    with (folder/(stem+"-"+label+".csv")).open("w",encoding="utf-8-sig",newline="") as output:
                        writer=csv.DictWriter(output,fieldnames=keys);writer.writeheader()
                        writer.writerows({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v for k,v in row.items()} for row in rows)
        return payload
    return record


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item,call):
    outcome=yield
    report=outcome.get_result()
    folder=case_folder(item)
    if folder is None:return
    entry=RECORDS.setdefault(item.nodeid,{"nodeid":item.nodeid,"file":str(folder.relative_to(directory(item.config))),"phases":{},
                                         "parameters":safe(getattr(getattr(item,"callspec",None),"params",{}))})
    entry["phases"][report.when]={"outcome":report.outcome,"duration_seconds":report.duration,
                                  "details":str(report.longrepr) if report.longrepr else None,
                                  "stdout":report.capstdout,"stderr":report.capstderr,"log":report.caplog}
    phases=entry["phases"]
    entry["outcome"]="failed" if any(p["outcome"]=="failed" for p in phases.values()) else "skipped" if any(p["outcome"]=="skipped" for p in phases.values()) else "passed" if phases.get("call",{}).get("outcome")=="passed" else "setup"
    (folder/"result.json").write_text(json.dumps(entry,ensure_ascii=False,indent=2),encoding="utf-8")


def pytest_sessionfinish(session,exitstatus):
    root=directory(session.config)
    if root is None:return
    root.mkdir(parents=True,exist_ok=True)
    values=list(RECORDS.values())
    summary={"exit_status":int(exitstatus),"tests":len(values),
             "counts":{k:sum(r["outcome"]==k for r in values) for k in ("passed","failed","skipped")},"cases":values}
    (root/"test-results.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    with (root/"test-index.csv").open("w",encoding="utf-8-sig",newline="") as output:
        writer=csv.DictWriter(output,fieldnames=["nodeid","outcome","file"]);writer.writeheader()
        writer.writerows({k:row[k] for k in writer.fieldnames} for row in values)
