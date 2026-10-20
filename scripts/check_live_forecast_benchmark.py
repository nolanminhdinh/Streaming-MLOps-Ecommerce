"""Small live HTTP forecast benchmark; fail if any response uses heuristic.

Replays an existing audit target/SKU so the idempotent audit insert adds no
forecast rows. This is a 20-request smoke benchmark, not a production load SLA.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import statistics
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sqlalchemy import text
from warehouse.etl.load import get_engine


def main():
    engine = get_engine()
    try:
        with engine.connect() as conn:
            existing = conn.execute(text("""SELECT p.sku, f.target_date FROM Fact_Forecast_Predictions f
                JOIN Dim_Products p USING(product_key) WHERE f.model_source='mlflow_registry'
                AND f.model_version='2' AND f.data_origin='public_anonymized_historical_csv'
                ORDER BY f.forecast_generated_at DESC LIMIT 1""")).mappings().one()
        body = json.dumps({"sku": existing["sku"], "from_date": str(existing["target_date"]), "to_date": str(existing["target_date"])}).encode()
        def request(_):
            started = time.perf_counter()
            with urlopen(Request("http://localhost:8000/predict/demand", data=body, headers={"Content-Type": "application/json"}), timeout=30) as response:
                payload = json.load(response)
                assert response.status == 200 and payload["model_source"] == "mlflow_registry"
                assert payload["model_version"] == "2"
            return (time.perf_counter() - started) * 1000
        request(0)
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as executor:
            times = sorted(executor.map(request, range(20)))
        elapsed = time.perf_counter() - started
        result = dict(status="PASS", mode="live_http", requests=20, concurrency=4, horizon_days=1,
                      model_source="mlflow_registry", model_version="2", errors=0,
                      elapsed_seconds=round(elapsed, 3), requests_per_second=round(20 / elapsed, 2),
                      p50_ms=round(statistics.median(times), 2), p95_ms=round(times[18], 2), max_ms=round(max(times), 2),
                      scope="warm single-SKU existing target replay; not a soak or capacity claim")
        path = ROOT / "docs/07-bao-cao-do-an/evidence/live_forecast_benchmark_2026-10-20.json"
        path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
