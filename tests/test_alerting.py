"""Tests for SlackAlerter and DashboardGenerator."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

import alerting.slack_alerter as alerter_module
from alerting.slack_alerter import SlackAlerter
from dashboards.dashboard_generator import DashboardGenerator
from models import AlertType, DriftAlert, ObservabilityEvent, Severity, SourceEnum, StatusEnum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_alert(
    model: str = "orders",
    alert_type: str = "ROW_COUNT_DRIFT",
    severity: str = "WARNING",
) -> DriftAlert:
    return DriftAlert(
        model_name=model,
        alert_type=alert_type,
        severity=severity,
        description="Test alert",
        current_value=10.0,
        expected_value=500.0,
    )


def make_event(model: str = "orders", rows: int = 500, exec_ms: float = 1200.0) -> ObservabilityEvent:
    return ObservabilityEvent(
        model_name=model,
        invocation_id="test-inv",
        execution_time_ms=exec_ms,
        rows_affected=rows,
        status=StatusEnum.success,
        source=SourceEnum.dbt,
    )


def _mock_httpx_client(status_code: int = 200):
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.text = "ok"
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    return mock_client


# ---------------------------------------------------------------------------
# SlackAlerter
# ---------------------------------------------------------------------------


class TestSlackAlerter:
    def setup_method(self):
        # Reset module-level dedup cache before each test
        alerter_module._dedup_cache.clear()

    def test_send_success(self):
        alert = make_alert()
        mock_client = _mock_httpx_client(200)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            result = SlackAlerter("https://hooks.slack.com/fake").send_alert(alert)

        assert result is True
        mock_client.post.assert_called_once()

    def test_send_failure_returns_false(self):
        alert = make_alert()
        mock_client = _mock_httpx_client(400)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            result = SlackAlerter("https://hooks.slack.com/fake").send_alert(alert)

        assert result is False

    def test_dedup_prevents_second_send(self):
        alert = make_alert()
        mock_client = _mock_httpx_client(200)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            alerter = SlackAlerter("https://hooks.slack.com/fake")
            first = alerter.send_alert(alert)
            second = alerter.send_alert(alert)

        assert first is True
        assert second is True
        # HTTP POST called only once
        assert mock_client.post.call_count == 1

    def test_different_alerts_both_sent(self):
        alert1 = make_alert(model="orders")
        alert2 = make_alert(model="customers")
        mock_client = _mock_httpx_client(200)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            alerter = SlackAlerter("https://hooks.slack.com/fake")
            alerter.send_alert(alert1)
            alerter.send_alert(alert2)

        assert mock_client.post.call_count == 2

    def test_http_error_returns_false(self):
        import httpx
        alert = make_alert()
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.HTTPError("timeout")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            result = SlackAlerter("https://hooks.slack.com/fake").send_alert(alert)

        assert result is False

    def test_payload_contains_model_name(self):
        alert = make_alert(model="fact_orders")
        captured = {}

        def fake_post(url, json=None, **kwargs):
            captured["json"] = json
            r = MagicMock()
            r.status_code = 200
            return r

        mock_client = MagicMock()
        mock_client.post.side_effect = fake_post
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            SlackAlerter("https://hooks.slack.com/fake").send_alert(alert)

        payload_str = str(captured["json"])
        assert "fact_orders" in payload_str

    def test_payload_contains_severity_emoji(self):
        alert = make_alert(severity="CRITICAL")
        captured = {}

        def fake_post(url, json=None, **kwargs):
            captured["json"] = json
            r = MagicMock()
            r.status_code = 200
            return r

        mock_client = MagicMock()
        mock_client.post.side_effect = fake_post
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            SlackAlerter("https://hooks.slack.com/fake").send_alert(alert)

        payload_str = str(captured["json"])
        assert ":rotating_light:" in payload_str

    def test_dashboard_url_in_payload(self):
        alert = make_alert()
        captured = {}

        def fake_post(url, json=None, **kwargs):
            captured["json"] = json
            r = MagicMock()
            r.status_code = 200
            return r

        mock_client = MagicMock()
        mock_client.post.side_effect = fake_post
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("alerting.slack_alerter.httpx.Client", return_value=mock_client):
            SlackAlerter(
                "https://hooks.slack.com/fake",
                base_url="http://my-platform:9090",
            ).send_alert(alert)

        payload_str = str(captured["json"])
        assert "my-platform:9090" in payload_str


# ---------------------------------------------------------------------------
# DashboardGenerator
# ---------------------------------------------------------------------------


class TestDashboardGenerator:
    def test_generates_html_file(self, tmp_path):
        events = [make_event("orders"), make_event("customers", exec_ms=2500.0)]
        alerts = [make_alert("orders")]
        output = str(tmp_path / "dashboard.html")

        path = DashboardGenerator().generate(events, alerts, output)

        assert pathlib.Path(path).exists()

    def test_html_contains_model_names(self, tmp_path):
        events = [make_event("dim_orders"), make_event("fact_revenue", exec_ms=3000.0)]
        alerts = [make_alert("dim_orders")]
        output = str(tmp_path / "dashboard.html")

        DashboardGenerator().generate(events, alerts, output)
        html = pathlib.Path(output).read_text(encoding="utf-8")

        assert "dim_orders" in html
        assert "fact_revenue" in html

    def test_html_contains_alert_type(self, tmp_path):
        events = [make_event("orders")]
        alerts = [make_alert("orders", alert_type="SILENT_FAILURE", severity="CRITICAL")]
        output = str(tmp_path / "dashboard.html")

        DashboardGenerator().generate(events, alerts, output)
        html = pathlib.Path(output).read_text(encoding="utf-8")

        assert "SILENT_FAILURE" in html

    def test_html_is_valid_html_structure(self, tmp_path):
        events = [make_event("orders")]
        alerts: list[DriftAlert] = []
        output = str(tmp_path / "dashboard.html")

        DashboardGenerator().generate(events, alerts, output)
        html = pathlib.Path(output).read_text(encoding="utf-8")

        assert "<!DOCTYPE html>" in html
        assert "<table>" in html
        assert "</html>" in html

    def test_no_events_renders_cleanly(self, tmp_path):
        output = str(tmp_path / "dashboard.html")
        DashboardGenerator().generate([], [], output)
        html = pathlib.Path(output).read_text(encoding="utf-8")
        assert "No model data available" in html
        assert "No open alerts" in html

    def test_creates_output_dir_if_missing(self, tmp_path):
        nested = tmp_path / "a" / "b" / "dashboard.html"
        DashboardGenerator().generate([], [], str(nested))
        assert nested.exists()

    def test_svg_chart_generated_for_multiple_events(self, tmp_path):
        events = [
            make_event("slow_model", exec_ms=float(ms))
            for ms in [1000, 2000, 3000, 4000, 5000]
        ]
        output = str(tmp_path / "dashboard.html")
        DashboardGenerator().generate(events, [], output)
        html = pathlib.Path(output).read_text(encoding="utf-8")
        assert "<svg" in html
        assert "slow_model" in html

    def test_multiple_alerts_all_shown(self, tmp_path):
        events = [make_event("a"), make_event("b")]
        alerts = [
            make_alert("a", alert_type="ROW_COUNT_DRIFT"),
            make_alert("b", alert_type="FRESHNESS"),
        ]
        output = str(tmp_path / "dashboard.html")
        DashboardGenerator().generate(events, alerts, output)
        html = pathlib.Path(output).read_text(encoding="utf-8")
        assert "ROW_COUNT_DRIFT" in html
        assert "FRESHNESS" in html
