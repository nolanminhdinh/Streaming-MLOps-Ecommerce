"""Defense-console contracts; no running Kafka, MinIO, or PostgreSQL required.

Run independently: python -m unittest tests.test_demo_web -v
These tests do not replace the live acceptance steps in demo-web.md.
"""
import json
from pathlib import Path
import random
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from scripts import demo_web_server as demo


class DemoDataTests(unittest.TestCase):
    def test_fixture_passes_real_transform_and_quarantines_only_injected_error(self):
        import pandas as pd
        from warehouse.etl.transform import unify_schema, clean_data
        from warehouse.etl.data_validation import DataValidator
        for seed in (11, 23, 37):
            with self.subTest(seed=seed):
                random.seed(seed)
                events = demo.make_events("WEB20261024090000abcdef", 50, True)
                orders = [event["payload"] for event in events if event["topic"] == "orders"]
                cleaned, _ = clean_data(unify_schema(pd.DataFrame(orders)), return_report=True, preserve_invalid=True)
                accepted, rejected, _ = DataValidator().validate(cleaned)
                self.assertEqual(len(accepted), 50)
                self.assertEqual(len(rejected), 1)
                self.assertIn("NON_POSITIVE_QUANTITY", str(rejected.iloc[0]["rejection_reasons"]))
                self.assertEqual({e["payload"]["sku"] for e in events if e["topic"] == "inventory"},
                                 {"WEB20261024090000abcdef-S", "WEB20261024090000abcdef-T"})
                self.assertTrue(all(row["order_status"] == "UNPAID" for row in orders))
                self.assertTrue(all(row.get("completed_time") is None for row in orders))


class ConsoleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root_patch = patch.object(demo, "RUNS", Path(self.directory.name))
        self.root_patch.start()
        self.console = demo.Console()

    def tearDown(self):
        self.console.stop.set()
        if self.console.job:
            self.console.job.join(timeout=3)
        self.root_patch.stop()
        self.directory.cleanup()

    def test_count_bounds_are_enforced_before_any_publish(self):
        for count in (0, 1, 51, True, "12", 2.5):
            with self.subTest(count=count), self.assertRaises(ValueError):
                self.console.start({"count": count})
        self.assertIsNone(self.console.job)

    def test_concurrent_batches_are_rejected(self):
        self.console.job = Mock(is_alive=lambda: True, join=lambda **_: None)
        with self.assertRaises(ValueError):
            self.console.start({"count": 2})

    def test_ack_failure_does_not_increment_count_and_is_persisted(self):
        producer = Mock()
        producer.send.return_value.get.side_effect = TimeoutError("ack missing")
        self.console.run = {"run_id": "WEB20261024090000abcdef", "status": "publishing",
                            "sent_orders": 0, "sent_inventory": 0, "acks": []}
        kafka = SimpleNamespace(KafkaProducer=Mock(return_value=producer))
        with patch.dict("sys.modules", {"kafka": kafka}), self.assertLogs(demo.LOG, level="ERROR"):
            self.console.publish([{"topic": "orders", "key": "id", "payload": {}, "invalid": False}])
        self.assertEqual(self.console.run["sent_orders"], 0)
        self.assertEqual(self.console.run["status"], "error")
        self.assertEqual(demo.Console().run["status"], "error")

    def test_stop_keeps_acknowledged_records(self):
        producer = Mock()
        def ack(**_):
            self.console.stop.set()
            return SimpleNamespace(partition=1, offset=22)
        producer.send.return_value.get.side_effect = ack
        self.console.run = {"run_id": "WEB20261024090000abcdef", "status": "publishing",
                            "sent_orders": 0, "sent_inventory": 0, "acks": []}
        kafka = SimpleNamespace(KafkaProducer=Mock(return_value=producer))
        event = {"topic": "orders", "key": "id", "payload": {}, "invalid": False}
        with patch.dict("sys.modules", {"kafka": kafka}):
            self.console.publish([event, event])
        self.assertEqual(self.console.run["sent_orders"], 1)
        self.assertEqual(self.console.run["acks"][0]["offset"], 22)
        self.assertEqual(self.console.run["status"], "stopped")
        self.assertEqual(producer.send.call_count, 1)

    def test_replay_preserves_payload_and_records_complete_baseline(self):
        original = [{"topic": "orders", "key": "label", "payload": {"order_sn": "label"}, "invalid": False}]
        self.console.run = {"run_id": "WEB20261024090000abcdef", "events": original,
                            "valid_orders": 2, "sent_orders": 2}
        self.console.snapshot = {"warehouse": {"run_count": 2}}
        with patch.object(self.console, "publish") as publish:
            self.console.start({}, replay=True)
            self.console.job.join(timeout=2)
            publish.assert_called_once_with(original)
        self.assertTrue(self.console.run["replay_baseline_complete"])
        self.assertEqual(self.console.run["events"], original)

    def test_new_run_never_displays_previous_run_evidence(self):
        self.console.run = {"run_id": "NEW"}
        self.console.snapshot = {"run_id": "OLD", "warehouse": {"ok": True, "run_count": 99}, "lake": {"ok": True, "raw_rows": 100}}
        state = self.console.state()
        self.assertFalse(state["warehouse"]["ok"])
        self.assertNotIn("raw_rows", state["lake"])

    def test_restart_marks_unfinished_publish_as_interrupted(self):
        self.console.run = {"run_id": "WEB20261024090000abcdef", "status": "publishing", "sent_orders": 3}
        self.console.save()
        recovered = demo.Console().run
        self.assertEqual(recovered["status"], "interrupted")
        self.assertEqual(recovered["sent_orders"], 3)


class HttpContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.handler = type("TestHandler", (demo.Handler,), {"console": Mock()})
        cls.handler.console.state.return_value = {"loading": False, "csrf": demo.TOKEN}
        cls.server = demo.ThreadingHTTPServer(("127.0.0.1", 0), cls.handler)
        cls.base = "http://127.0.0.1:" + str(cls.server.server_port)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)

    def request(self, path, data=None, headers=None):
        return urlopen(Request(self.base + path, data=json.dumps(data).encode() if data is not None else None,
                               headers=headers or {}), timeout=3)

    def test_assets_and_report_work_without_pipeline_and_hide_token(self):
        for path in ("/", "/app.js", "/style.css", "/api/state", "/api/report"):
            with self.subTest(path=path), self.request(path) as response:
                self.assertEqual(response.status, 200)
                if path == "/api/report":
                    self.assertNotIn("csrf", json.load(response))

    def test_cross_origin_or_missing_token_cannot_publish(self):
        for headers in ({}, {"X-Demo-Token": demo.TOKEN, "Origin": "https://elsewhere.example"}):
            with self.subTest(headers=headers), self.assertRaises(HTTPError) as caught:
                self.request("/api/demo/start", {"count": 12}, headers)
            self.assertEqual(caught.exception.code, 403)

    def test_proxy_keeps_original_api_failure_visible(self):
        with patch.object(demo, "api_call", return_value={"ok": False, "status": 400, "data": {"detail": "SKU thiếu lịch sử"}}):
            with self.assertRaises(HTTPError) as caught:
                self.request("/api/predict", {"sku": "SKU-1", "from_date": "2026-10-24", "days": 7}, {"X-Demo-Token": demo.TOKEN})
            self.assertEqual(caught.exception.code, 502)
            self.assertEqual(json.load(caught.exception)["data"]["detail"], "SKU thiếu lịch sử")

    def test_invalid_dates_and_negative_stock_never_call_serving(self):
        with patch.object(demo, "api_call") as api:
            for path, body in (("/api/predict", {"sku": "SKU", "from_date": "invalid"}),
                               ("/api/inventory", {"sku": "SKU", "current_stock": -1})):
                with self.subTest(path=path), self.assertRaises(HTTPError) as caught:
                    self.request(path, body, {"X-Demo-Token": demo.TOKEN})
                self.assertEqual(caught.exception.code, 400)
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
