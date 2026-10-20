"""Inspect serving and monitoring without train, reload, or configuration updates.

Replays one forecast already present in the audit table, when available; the API
may attempt its normal idempotent forecast audit insert. Reports row counts.
"""
import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import text
from data_simulator.data_simulator import PRODUCT_CATALOG
from serving.app.inventory_service import InventoryService
from serving.app.model_loader import ModelManager
from serving.app.schemas import ReorderAlertRequest

ROOT = Path(__file__).resolve().parents[1]


def http(base, path, body=None):
    req = Request(base + path, data=None if body is None else json.dumps(body).encode(),
                  headers={"Content-Type": "application/json"})
    try:
        response = urlopen(req, timeout=45)
    except HTTPError as exc:
        response = exc
    with response:
        content = response.read().decode()
        try:
            payload = json.loads(content)
        except ValueError:
            payload = {"non_json_body": content[:200]}
        return {"http_status": response.status, "body": payload}


def offline():
    from pydantic import ValidationError
    try:
        ReorderAlertRequest(lead_time_days=-1, service_level=2.0, current_stocks={"X": -5})
    except ValidationError as exc:
        invalid_parameters_rejected = len(exc.errors()) == 3
    else:
        invalid_parameters_rejected = False
    assert invalid_parameters_rejected
    manager = ModelManager()
    marker = object()
    manager.model = marker
    manager.feature_spec = {"feature_columns": ["daily_demand"]}
    manager.model_source = "local_joblib"
    manager.is_loaded = True
    with patch.object(ModelManager, "_load_model_candidate", return_value=None):
        loaded = manager.load_model()
    assert loaded is False and manager.model is marker
    # This formula helper has no need to construct a real manager/repository.
    service = object.__new__(InventoryService)
    ss, rop = service.calculate_policy(0, 0)
    assert (ss, rop) == (1, 2)
    return {"post_inventory_schema_rejects_invalid_parameters": invalid_parameters_rejected,
            "failed_reload_preserves_existing_model": manager.model is marker,
            "zero_demand_zero_sigma_policy": {"safety_stock": ss, "reorder_point": rop}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    from warehouse.etl.load import get_engine
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            count_before = conn.execute(text("SELECT count(*) FROM Fact_Forecast_Predictions")).scalar_one()
            recorded = conn.execute(text("""SELECT p.sku, f.target_date FROM Fact_Forecast_Predictions f
                JOIN Dim_Products p USING(product_key)
                WHERE f.model_source='mlflow_registry' AND f.model_version='2'
                  AND f.data_origin='public_anonymized_historical_csv'
                ORDER BY f.forecast_generated_at DESC LIMIT 1""")).mappings().first()
            stock = conn.execute(text("""SELECT p.sku, i.stock_on_hand, d.full_date
                FROM Fact_Inventory_Daily i JOIN Dim_Products p USING(product_key)
                JOIN Dim_Dates d USING(date_key) ORDER BY d.full_date DESC, i.loaded_at DESC LIMIT 1""")).mappings().first()
        api = "http://localhost:8000"
        result = {"audit_date": str(args.audit_date), "offline": offline(), "api": {}}
        for endpoint in ["/health", "/ready", "/model/metadata", "/training/status", "/metrics?format=json"]:
            result["api"][endpoint] = http(api, endpoint)
        known = recorded["sku"] if recorded else None
        if recorded:
            body = {"sku": known, "from_date": str(recorded["target_date"]),
                    "to_date": str(recorded["target_date"]), "confidence_interval": True}
            response = http(api, "/predict/demand", body)
            response["replayed_existing_audit_key"] = True
            result["model_forecast"] = response
        fallback_sku = PRODUCT_CATALOG[0]["sku"]
        for label, sku in [("simulator", fallback_sku), ("unknown", "AUDIT-NOT-IN-CATALOG")]:
            result[label + "_forecast"] = http(api, "/predict/demand",
                {"sku": sku, "from_date": str(args.audit_date), "to_date": str(args.audit_date)})
        result["invalid_date_range"] = http(api, "/predict/demand",
            {"sku": fallback_sku, "from_date": "2026-10-20", "to_date": "2026-10-19"})
        result["overlong_date_range"] = http(api, "/predict/demand",
            {"sku": fallback_sku, "from_date": "2026-01-01", "to_date": "2026-12-31"})
        chosen = list(dict.fromkeys([sku for sku in [known, fallback_sku, stock["sku"] if stock else None] if sku]))
        result["inventory_selected"] = http(api, "/inventory/reorder-alert", {"skus": chosen})
        if stock:
            result["stock_db_reference"] = dict(stock)
            result["stock_db_reference"]["age_days_at_audit"] = (args.audit_date - stock["full_date"]).days
        if known:
            result["inventory_request_override"] = http(api, "/inventory/reorder-alert",
                {"skus": [known], "current_stocks": {known: 0}, "lead_time_days": 3, "service_level": 0.95})
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            count_after = conn.execute(text("SELECT count(*) FROM Fact_Forecast_Predictions")).scalar_one()
        result["forecast_audit_rows"] = {"before": count_before, "after": count_after,
                                          "difference": count_after - count_before}
        ready = result["api"]["/ready"]["body"]
        if ready.get("history_end"):
            result["history_age_days_at_audit"] = (args.audit_date - date.fromisoformat(ready["history_end"])).days
        targets = http("http://localhost:9090", "/api/v1/targets")
        result["prometheus_targets"] = [{"job": t.get("labels", {}).get("job"), "health": t.get("health"),
                                         "last_error": t.get("lastError")}
                                        for t in targets["body"].get("data", {}).get("activeTargets", [])]
        rules = http("http://localhost:9090", "/api/v1/rules")
        result["prometheus_rules"] = [{"name": rule.get("name"), "state": rule.get("state"),
                                       "health": rule.get("health"), "query": rule.get("query")}
                                      for group in rules["body"].get("data", {}).get("groups", [])
                                      for rule in group.get("rules", [])]
        managers = http("http://localhost:9090", "/api/v1/alertmanagers")
        result["active_alertmanagers"] = len(managers["body"].get("data", {}).get("activeAlertmanagers", []))
        result["grafana_health"] = http("http://localhost:3000", "/api/health")
        result["saved_reports"] = {}
        for filename in ["drift_summary.json", "retraining_log.json"]:
            path = ROOT / "data/monitoring_reports" / filename
            if path.exists():
                saved = json.loads(path.read_text(encoding="utf-8"))
                result["saved_reports"][filename] = {k: saved.get(k) for k in
                    ["status", "data_source", "feature_data_source", "data_origin", "evaluated_at", "timestamp",
                     "drift_detected", "target_drift_detected", "drift_share", "stage", "serving_reloaded"] if k in saved}
        destination = ROOT / "docs/07-bao-cao-do-an/evidence" / f"serving_monitoring_{args.audit_date}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
        compact = {k: v for k, v in result.items() if k not in ["model_forecast", "simulator_forecast", "unknown_forecast", "inventory_selected", "inventory_request_override"]}
        for label in ["model_forecast", "simulator_forecast", "unknown_forecast"]:
            if label in result:
                r = result[label]
                compact[label] = {"http_status": r["http_status"], **{k: r["body"].get(k) for k in
                                  ["model_source", "history_source", "model_version", "total_predicted_demand"]}}
        inventory = result["inventory_selected"]
        compact["inventory_selected"] = {"http_status": inventory["http_status"], "items": [
            {k: item.get(k) for k in ["sku", "current_stock", "stock_source", "demand_source", "alert_level"]}
            for item in inventory["body"].get("alerts", [])]}
        print(json.dumps(compact, indent=2, ensure_ascii=False, default=str))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
