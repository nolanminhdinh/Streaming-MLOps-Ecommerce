"""Recovery after startup Registry failure must not block readiness or race loads."""
import threading
from unittest import mock

from serving.app import model_loader
from serving.app import main


def initialized_fallback(candidate):
    candidate.is_loaded = True
    candidate.feature_spec = {"feature_columns": ["daily_demand"]}


def test_startup_failure_recovers_without_blocking_readiness():
    manager = model_loader.ModelManager()
    started, release = threading.Event(), threading.Event()
    calls = []

    def load(candidate):
        calls.append(1)
        initialized_fallback(candidate)
        if len(calls) > 1:
            started.set()
            assert release.wait(5)
            candidate.model = object()
            candidate.model_source = model_loader.SOURCE_MLFLOW

    repo = mock.Mock()
    repo.available.return_value = True
    from datetime import date
    repo.get_last_order_date.return_value = date(2026, 8, 11)
    with mock.patch.dict("os.environ", {"MLFLOW_TRACKING_URI": "http://registry.test"}), \
         mock.patch.object(model_loader.ModelManager, "_instance", manager), \
         mock.patch.object(model_loader.ModelManager, "_load_model_candidate", load), \
         mock.patch.object(model_loader.time, "monotonic", return_value=100) as clock, \
         mock.patch.object(main, "get_repository", return_value=repo):
        assert not manager.load_model()
        clock.return_value = 104
        assert main.readiness_check().status_code == 503
        assert len(calls) == 1
        clock.return_value = 105
        try:
            # This returns while the recovery worker is deliberately blocked.
            assert main.readiness_check().status_code == 503
            assert started.wait(2)
            for _ in range(10):
                assert main.readiness_check().status_code == 503
            assert len(calls) == 2
        finally:
            release.set()
            with manager._load_lock:
                pass
        assert main.readiness_check()["status"] == "ready"
        assert manager.model_source == model_loader.SOURCE_MLFLOW
        assert manager._retry_delay == manager._retry_initial


def test_backoff_is_capped_and_early_probes_do_not_load():
    with mock.patch.dict("os.environ", {"MLFLOW_TRACKING_URI": "http://registry.test",
                                         "MODEL_LOAD_RETRY_INITIAL_SECONDS": "5",
                                         "MODEL_LOAD_RETRY_MAX_SECONDS": "12"}), \
         mock.patch.object(model_loader.time, "monotonic", return_value=100) as clock, \
         mock.patch.object(model_loader.ModelManager, "_load_model_candidate", initialized_fallback):
        manager = model_loader.ModelManager()
        for now, wait in [(100, 5), (105, 10), (115, 12), (127, 12)]:
            clock.return_value = now
            assert not manager.load_model()
            assert manager._next_retry_at == now + wait
            clock.return_value = now + wait - 1
            with mock.patch.object(model_loader.threading, "Thread") as worker:
                manager.retry_if_due()
                worker.assert_not_called()


def test_healthy_model_is_not_replaced_and_failed_reload_keeps_lock():
    manager = model_loader.ModelManager()
    existing, lock = object(), manager._load_lock
    manager.model, manager.is_loaded = existing, True
    with mock.patch.dict("os.environ", {"MLFLOW_TRACKING_URI": "http://registry.test"}), \
         mock.patch.object(model_loader.ModelManager, "_load_model_candidate", initialized_fallback) as load:
        manager.retry_if_due()
        assert not manager.load_model()
    assert manager.model is existing
    assert manager._load_lock is lock


def test_unexpected_loader_error_releases_lock_and_schedules_retry():
    manager = model_loader.ModelManager()
    with mock.patch.object(model_loader.ModelManager, "_load_model_candidate", side_effect=RuntimeError("transient")):
        assert not manager.load_model()
    assert manager.is_loaded
    assert manager._next_retry_at > 0
    assert manager._load_lock.acquire(blocking=False)
    manager._load_lock.release()
