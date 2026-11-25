"""Failure contracts for the core pipeline without NiFi or live services."""
import json
from types import SimpleNamespace
from unittest import mock

import pytest

from ingestion import producer, consumer_to_minio as orders
from scripts import run_pipeline_worker as worker


def test_producer_propagates_broker_rejection():
    publisher = mock.Mock()
    publisher.send.return_value.get.side_effect = TimeoutError("broker down")
    with pytest.raises(TimeoutError, match="broker down"):
        producer.send_event(publisher, {"order_sn": "A"})
    publisher.send.return_value.get.assert_called_once_with(timeout=30)


def test_storage_failure_never_commits_order_offsets(monkeypatch):
    consumer = mock.MagicMock()
    consumer.__iter__.return_value = iter([SimpleNamespace(value={"order_sn": "A"})])
    monkeypatch.setattr(orders, "_running", True)
    monkeypatch.setattr(orders, "BATCH_SIZE", 1)
    monkeypatch.setattr(orders, "build_consumer", lambda: consumer)
    monkeypatch.setattr(orders, "build_minio_client", mock.Mock())
    monkeypatch.setattr(orders, "ensure_bucket", mock.Mock())
    monkeypatch.setattr(orders, "flush_batch_to_minio", mock.Mock(side_effect=OSError("lake down")))
    with pytest.raises(OSError, match="lake down"):
        orders.main()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


@pytest.mark.parametrize("exit_code", [0, 1, -15])
def test_exited_consumer_is_failure_even_on_clean_exit(exit_code):
    with pytest.raises(RuntimeError, match="Consumer exited"):
        worker.check_consumers([mock.Mock(poll=mock.Mock(return_value=exit_code))])


def test_worker_invalidates_healthy_status_before_failed_startup(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "ROOT", tmp_path)
    path = tmp_path / "data/pipeline/status.json"
    worker.write_state(path, {"status": "healthy"})
    monkeypatch.setattr(worker, "migrate", mock.Mock(side_effect=OSError("postgres down")))
    monkeypatch.setattr(worker.signal, "signal", mock.Mock())
    with pytest.raises(OSError, match="postgres down"):
        worker.main()
    assert json.loads(path.read_text())["status"] == "starting"


def test_consumer_dying_during_etl_never_reports_healthy(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "ROOT", tmp_path)
    monkeypatch.setattr(worker.signal, "signal", mock.Mock())
    monkeypatch.setattr(worker, "STOP", mock.Mock(is_set=mock.Mock(side_effect=[False, False])))
    monkeypatch.setattr(worker, "migrate", mock.Mock())
    monkeypatch.setattr(worker, "get_engine", mock.MagicMock())
    monkeypatch.setattr(worker, "_install_views", mock.Mock())
    process = mock.Mock()
    process.poll.side_effect = [None, 1, 1, 1]
    other = mock.Mock(poll=mock.Mock(return_value=None))
    monkeypatch.setattr(worker.subprocess, "Popen", mock.Mock(side_effect=[process, other]))
    monkeypatch.setattr(worker, "run_cycle", mock.Mock(return_value={"pending_objects": 0}))
    writes = []
    monkeypatch.setattr(worker, "write_state", lambda path, value: writes.append(value["status"]))
    with pytest.raises(RuntimeError, match="Consumer exited"):
        worker.main()
    assert "healthy" not in writes
    assert writes == ["starting", "retrying", "stopped"]
    other.terminate.assert_called_once()
