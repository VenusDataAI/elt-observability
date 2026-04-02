"""Tests for all four detectors."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from detectors.freshness_detector import FreshnessDetector
from detectors.row_count_drift import RowCountDriftDetector
from detectors.schema_drift import SchemaDriftDetector
from detectors.silent_failure_detector import SilentFailureDetector
from models import AlertType, ChangeType, FreshnessAlert, Severity, StatusEnum, SourceEnum
from models import ObservabilityEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_event(
    model: str = "orders",
    rows: int = 500,
    status: str = "success",
    source: str = "dbt",
    exec_ms: float = 1000.0,
) -> ObservabilityEvent:
    return ObservabilityEvent(
        model_name=model,
        invocation_id="test-inv",
        execution_time_ms=exec_ms,
        rows_affected=rows,
        status=status,
        source=source,
    )


NORMAL_HISTORY = [make_event(rows=r) for r in [500, 510, 495, 505, 498, 502, 508, 497, 503, 501]]
CURRENT_NORMAL = make_event(rows=504)
CURRENT_DRIFT_MILD = make_event(rows=450)   # moderate deviation
CURRENT_DRIFT_EXTREME = make_event(rows=10)  # extreme deviation
CURRENT_SILENT = make_event(rows=0, status="success")


# ---------------------------------------------------------------------------
# RowCountDriftDetector
# ---------------------------------------------------------------------------


class TestRowCountDriftDetector:
    det = RowCountDriftDetector()

    def test_no_drift_returns_none(self):
        result = self.det.detect("orders", CURRENT_NORMAL, NORMAL_HISTORY)
        assert result is None

    def test_mild_drift_returns_warning(self):
        # Build history with tight cluster, then a mild outlier
        history = [make_event(rows=r) for r in [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000]]
        current = make_event(rows=1030)  # 3 stddev when stddev ~ 0 but let's use proper spread
        # Use spread of 10 to get real stddev
        history2 = [make_event(rows=r) for r in [490, 510, 495, 505, 498, 502, 508, 497, 503, 492]]
        current2 = make_event(rows=450)  # moderate outlier
        result = self.det.detect("orders", current2, history2, z_threshold=2.5)
        assert result is not None
        assert result.alert_type == AlertType.ROW_COUNT_DRIFT

    def test_extreme_drift_returns_critical(self):
        result = self.det.detect("orders", CURRENT_DRIFT_EXTREME, NORMAL_HISTORY, z_threshold=2.5)
        assert result is not None
        assert result.severity == Severity.CRITICAL

    def test_insufficient_history_returns_none(self):
        result = self.det.detect("orders", CURRENT_NORMAL, [make_event(rows=500)])
        assert result is None

    def test_empty_history_returns_none(self):
        result = self.det.detect("orders", CURRENT_NORMAL, [])
        assert result is None

    def test_zero_stddev_same_value_returns_none(self):
        history = [make_event(rows=500)] * 5
        current = make_event(rows=500)
        result = self.det.detect("orders", current, history)
        assert result is None

    def test_zero_stddev_different_value_returns_warning(self):
        history = [make_event(rows=500)] * 5
        current = make_event(rows=300)
        result = self.det.detect("orders", current, history)
        assert result is not None
        assert result.severity == Severity.WARNING

    def test_window_limits_history(self):
        # Large history but window=3, current deviates from last 3 only
        big_history = [make_event(rows=r) for r in [100] * 10 + [500, 500, 500]]
        current = make_event(rows=100)
        # With window=3, mean~500, current=100 → should drift
        result = self.det.detect("orders", current, big_history, window=3, z_threshold=1.0)
        assert result is not None

    def test_alert_fields_populated(self):
        result = self.det.detect("orders", CURRENT_DRIFT_EXTREME, NORMAL_HISTORY)
        assert result is not None
        assert result.model_name == "orders"
        assert result.current_value == pytest.approx(10.0)
        assert result.expected_value > 0


# ---------------------------------------------------------------------------
# SchemaDriftDetector
# ---------------------------------------------------------------------------


BASE_COLUMNS = {
    "order_id": {"name": "order_id", "data_type": "integer"},
    "amount": {"name": "amount", "data_type": "numeric"},
    "customer_id": {"name": "customer_id", "data_type": "integer"},
}


def make_manifest(columns: dict, model_name: str = "orders", tmp_path=None):
    import json, pathlib
    data = {
        "nodes": {
            f"model.project.{model_name}": {
                "name": model_name,
                "columns": columns,
            }
        }
    }
    if tmp_path is not None:
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        return str(p)
    return data


class TestSchemaDriftDetector:
    det = SchemaDriftDetector()

    def test_no_change_returns_empty(self, tmp_path):
        path = make_manifest(BASE_COLUMNS, tmp_path=tmp_path)
        alerts = self.det.detect("orders", path, BASE_COLUMNS)
        assert alerts == []

    def test_added_column_returns_warning(self, tmp_path):
        new_cols = {**BASE_COLUMNS, "discount": {"name": "discount", "data_type": "numeric"}}
        path = make_manifest(new_cols, tmp_path=tmp_path)
        alerts = self.det.detect("orders", path, BASE_COLUMNS)
        assert len(alerts) == 1
        assert alerts[0].alert_type == AlertType.SCHEMA_DRIFT
        assert alerts[0].severity == Severity.WARNING
        assert "discount" in alerts[0].description

    def test_removed_column_returns_warning(self, tmp_path):
        new_cols = {k: v for k, v in BASE_COLUMNS.items() if k != "amount"}
        path = make_manifest(new_cols, tmp_path=tmp_path)
        alerts = self.det.detect("orders", path, BASE_COLUMNS)
        assert len(alerts) == 1
        assert "amount" in alerts[0].description
        assert alerts[0].severity == Severity.WARNING

    def test_type_changed_returns_critical(self, tmp_path):
        new_cols = {
            **BASE_COLUMNS,
            "amount": {"name": "amount", "data_type": "varchar"},  # was numeric
        }
        path = make_manifest(new_cols, tmp_path=tmp_path)
        alerts = self.det.detect("orders", path, BASE_COLUMNS)
        assert len(alerts) == 1
        assert alerts[0].severity == Severity.CRITICAL
        assert "amount" in alerts[0].description

    def test_multiple_changes_multiple_alerts(self, tmp_path):
        new_cols = {
            "order_id": {"name": "order_id", "data_type": "bigint"},  # type changed
            "new_col": {"name": "new_col", "data_type": "text"},       # added
            # amount removed, customer_id removed
        }
        path = make_manifest(new_cols, tmp_path=tmp_path)
        alerts = self.det.detect("orders", path, BASE_COLUMNS)
        assert len(alerts) == 4  # 1 type_changed + 1 added + 2 removed

    def test_missing_manifest_returns_empty(self, tmp_path):
        alerts = self.det.detect("orders", str(tmp_path / "missing.json"), BASE_COLUMNS)
        assert alerts == []


# ---------------------------------------------------------------------------
# FreshnessDetector
# ---------------------------------------------------------------------------


class TestFreshnessDetector:
    det = FreshnessDetector()
    now = datetime.now(timezone.utc)

    def test_fresh_model_returns_none(self):
        last_run = self.now - timedelta(hours=1)
        result = self.det.detect("orders", last_run, sla_hours=4, layer="silver")
        assert result is None

    def test_stale_model_returns_warning(self):
        last_run = self.now - timedelta(hours=5)
        result = self.det.detect("orders", last_run, sla_hours=4, layer="silver")
        assert result is not None
        assert isinstance(result, FreshnessAlert)
        assert result.severity == Severity.WARNING
        assert result.layer == "silver"
        assert result.sla_hours == 4

    def test_very_stale_model_returns_critical(self):
        last_run = self.now - timedelta(hours=10)
        result = self.det.detect("orders", last_run, sla_hours=4, layer="gold")
        assert result is not None
        assert result.severity == Severity.CRITICAL

    def test_exactly_at_sla_boundary_returns_none(self):
        last_run = self.now - timedelta(hours=4, seconds=-1)
        result = self.det.detect("orders", last_run, sla_hours=4)
        assert result is None

    def test_naive_datetime_handled(self):
        naive_dt = datetime.utcnow() - timedelta(hours=6)
        result = self.det.detect("orders", naive_dt, sla_hours=4)
        assert result is not None

    def test_alert_fields_correct(self):
        last_run = self.now - timedelta(hours=9)
        result = self.det.detect("model_x", last_run, sla_hours=4, layer="bronze")
        assert result is not None
        assert result.model_name == "model_x"
        assert result.alert_type == AlertType.FRESHNESS
        assert result.actual_age_hours > 8.9
        assert result.current_value == pytest.approx(result.actual_age_hours)
        assert result.expected_value == 4.0


# ---------------------------------------------------------------------------
# SilentFailureDetector
# ---------------------------------------------------------------------------


class TestSilentFailureDetector:
    det = SilentFailureDetector()

    def test_normal_success_with_rows_returns_none(self):
        current = make_event(rows=500, status="success")
        result = self.det.detect("orders", current, NORMAL_HISTORY)
        assert result is None

    def test_error_status_ignored(self):
        current = make_event(rows=0, status="error")
        result = self.det.detect("orders", current, NORMAL_HISTORY)
        assert result is None

    def test_silent_failure_detected(self):
        current = make_event(rows=0, status="success")
        result = self.det.detect("orders", current, NORMAL_HISTORY)
        assert result is not None
        assert result.alert_type == AlertType.SILENT_FAILURE
        assert result.severity == Severity.CRITICAL
        assert result.current_value == 0.0
        assert result.expected_value > 100

    def test_low_baseline_not_flagged(self):
        low_history = [make_event(rows=r) for r in [10, 20, 15, 8, 12]]
        current = make_event(rows=0, status="success")
        result = self.det.detect("orders", current, low_history, min_expected_rows=100)
        assert result is None

    def test_insufficient_history_returns_none(self):
        current = make_event(rows=0, status="success")
        result = self.det.detect("orders", current, [make_event(rows=500)])
        assert result is None

    def test_empty_history_returns_none(self):
        current = make_event(rows=0, status="success")
        result = self.det.detect("orders", current, [])
        assert result is None

    def test_alert_description_contains_mean(self):
        current = make_event(rows=0, status="success")
        result = self.det.detect("orders", current, NORMAL_HISTORY)
        assert result is not None
        assert "500" in result.description or "50" in result.description
