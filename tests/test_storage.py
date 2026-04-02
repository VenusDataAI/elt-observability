"""Tests for MetricsStore and config module."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import config as config_module
from config import get_config, reset_config
from models import AlertType, DriftAlert, FreshnessAlert, ObservabilityEvent, Severity, SourceEnum, StatusEnum
from storage.metrics_store import MetricsStore


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def setup_method(self):
        reset_config()

    def test_load_config(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("drift_z_threshold: 3.0\nbase_url: http://test\n")
        cfg = get_config(str(cfg_file))
        assert cfg["drift_z_threshold"] == 3.0
        assert cfg["base_url"] == "http://test"

    def test_singleton_caches(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("x: 1\n")
        a = get_config(str(cfg_file))
        b = get_config(str(cfg_file))
        assert a is b

    def test_reset_clears_cache(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("x: 1\n")
        get_config(str(cfg_file))
        reset_config()
        assert config_module._config is None


# ---------------------------------------------------------------------------
# MetricsStore
# ---------------------------------------------------------------------------


def make_event(model: str = "orders", rows: int = 500) -> ObservabilityEvent:
    return ObservabilityEvent(
        model_name=model,
        invocation_id="test-inv",
        execution_time_ms=1200.0,
        rows_affected=rows,
        status=StatusEnum.success,
        source=SourceEnum.dbt,
    )


def make_alert(model: str = "orders") -> DriftAlert:
    return DriftAlert(
        model_name=model,
        alert_type=AlertType.ROW_COUNT_DRIFT,
        severity=Severity.WARNING,
        description="test",
        current_value=10.0,
        expected_value=500.0,
    )


class TestMetricsStore:
    def test_save_and_retrieve_event(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        event = make_event("orders", rows=300)
        store.save_event(event)

        history = store.get_model_history("orders")
        assert len(history) == 1
        assert history[0].model_name == "orders"
        assert history[0].rows_affected == 300
        assert history[0].status == StatusEnum.success

    def test_last_n_limits_results(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        for i in range(15):
            store.save_event(make_event("orders", rows=i * 10))

        history = store.get_model_history("orders", last_n=5)
        assert len(history) == 5

    def test_model_history_filters_by_model(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        store.save_event(make_event("orders"))
        store.save_event(make_event("customers"))

        assert len(store.get_model_history("orders")) == 1
        assert len(store.get_model_history("customers")) == 1

    def test_save_and_retrieve_alert(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        alert = make_alert("orders")
        store.save_alert(alert)

        open_alerts = store.get_open_alerts()
        assert len(open_alerts) == 1
        assert open_alerts[0].model_name == "orders"
        assert open_alerts[0].resolved_at is None

    def test_resolve_alert(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        alert = make_alert()
        store.save_alert(alert)

        result = store.resolve_alert(str(alert.alert_id))
        assert result is True
        assert store.get_open_alerts() == []

    def test_resolve_nonexistent_returns_false(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        result = store.resolve_alert("00000000-0000-0000-0000-000000000000")
        assert result is False

    def test_save_freshness_alert(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        alert = FreshnessAlert(
            model_name="bronze_events",
            severity=Severity.WARNING,
            description="stale",
            current_value=5.0,
            expected_value=2.0,
            layer="bronze",
            sla_hours=2.0,
            actual_age_hours=5.0,
        )
        store.save_alert(alert)

        open_alerts = store.get_open_alerts()
        assert len(open_alerts) == 1
        fa = open_alerts[0]
        assert isinstance(fa, FreshnessAlert)
        assert fa.layer == "bronze"
        assert fa.sla_hours == 2.0

    def test_schema_created_on_first_run(self, tmp_path):
        db = tmp_path / "metrics.db"
        assert not db.exists()
        MetricsStore(str(db))
        assert db.exists()

    def test_empty_history_returns_empty_list(self, tmp_path):
        store = MetricsStore(str(tmp_path / "metrics.db"))
        assert store.get_model_history("nonexistent") == []
