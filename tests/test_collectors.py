"""Tests for all three collectors."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from collector.dbt_collector import DbtCollector
from collector.airflow_collector import AirflowCollector
from collector.redshift_collector import RedshiftCollector
from models import SourceEnum, StatusEnum


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RUN_RESULTS = {
    "metadata": {"invocation_id": "inv-abc-001"},
    "results": [
        {
            "unique_id": "model.my_project.orders",
            "status": "success",
            "execution_time": 1.5,
            "adapter_response": {"rows_affected": 500},
        },
        {
            "unique_id": "model.my_project.customers",
            "status": "error",
            "execution_time": 0.3,
            "adapter_response": {"rows_affected": None},
        },
        {
            "unique_id": "model.my_project.products",
            "status": "skipped",
            "execution_time": 0.0,
            "adapter_response": {},
        },
    ],
}

SAMPLE_MANIFEST = {
    "nodes": {
        "model.my_project.orders": {
            "name": "orders",
            "compiled_sql": "SELECT * FROM raw_orders",
            "columns": {
                "order_id": {"name": "order_id", "data_type": "integer"},
                "amount": {"name": "amount", "data_type": "numeric"},
            },
        },
        "model.my_project.customers": {
            "name": "customers",
            "compiled_sql": "SELECT * FROM raw_customers",
            "columns": {},
        },
    }
}


# ---------------------------------------------------------------------------
# DbtCollector
# ---------------------------------------------------------------------------


class TestDbtCollector:
    def test_basic_collect(self, tmp_path):
        rr = tmp_path / "run_results.json"
        mf = tmp_path / "manifest.json"
        rr.write_text(json.dumps(SAMPLE_RUN_RESULTS), encoding="utf-8")
        mf.write_text(json.dumps(SAMPLE_MANIFEST), encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(mf))

        assert len(events) == 3
        orders = next(e for e in events if e.model_name == "orders")
        assert orders.rows_affected == 500
        assert orders.status == StatusEnum.success
        assert orders.execution_time_ms == pytest.approx(1500.0)
        assert orders.source == SourceEnum.dbt
        assert orders.invocation_id == "inv-abc-001"
        assert orders.compiled_sql_hash is not None

    def test_none_rows_affected_defaults_to_zero(self, tmp_path):
        rr = tmp_path / "run_results.json"
        mf = tmp_path / "manifest.json"
        rr.write_text(json.dumps(SAMPLE_RUN_RESULTS), encoding="utf-8")
        mf.write_text(json.dumps(SAMPLE_MANIFEST), encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(mf))
        customers = next(e for e in events if e.model_name == "customers")
        assert customers.rows_affected == 0
        assert customers.status == StatusEnum.error

    def test_skipped_status_parsed(self, tmp_path):
        rr = tmp_path / "run_results.json"
        mf = tmp_path / "manifest.json"
        rr.write_text(json.dumps(SAMPLE_RUN_RESULTS), encoding="utf-8")
        mf.write_text(json.dumps(SAMPLE_MANIFEST), encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(mf))
        products = next(e for e in events if e.model_name == "products")
        assert products.status == StatusEnum.skipped

    def test_missing_run_results_returns_empty(self, tmp_path):
        events = DbtCollector().collect(
            str(tmp_path / "missing.json"),
            str(tmp_path / "manifest.json"),
        )
        assert events == []

    def test_missing_manifest_still_collects(self, tmp_path):
        rr = tmp_path / "run_results.json"
        rr.write_text(json.dumps(SAMPLE_RUN_RESULTS), encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(tmp_path / "no_manifest.json"))
        assert len(events) == 3
        # No manifest → no sql hash
        assert all(e.compiled_sql_hash is None for e in events)

    def test_invalid_json_returns_empty(self, tmp_path):
        rr = tmp_path / "run_results.json"
        rr.write_text("not valid json", encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(tmp_path / "manifest.json"))
        assert events == []

    def test_no_results_key(self, tmp_path):
        rr = tmp_path / "run_results.json"
        rr.write_text(json.dumps({"metadata": {"invocation_id": "x"}}), encoding="utf-8")
        mf = tmp_path / "manifest.json"
        mf.write_text(json.dumps({"nodes": {}}), encoding="utf-8")

        events = DbtCollector().collect(str(rr), str(mf))
        assert events == []


# ---------------------------------------------------------------------------
# AirflowCollector
# ---------------------------------------------------------------------------


def _make_airflow_row(
    dag_id="elt_pipeline",
    task_id="transform_orders",
    run_id="run_001",
    state="success",
    duration=72.3,
    start_date=None,
):
    row = MagicMock()
    row.dag_id = dag_id
    row.task_id = task_id
    row.run_id = run_id
    row.state = state
    row.duration = duration
    row.start_date = start_date or datetime(2026, 4, 1, 1, 0, tzinfo=timezone.utc)
    return row


def _mock_airflow_engine(rows):
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = rows
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine


class TestAirflowCollector:
    def test_basic_collect(self):
        row = _make_airflow_row()
        mock_engine = _mock_airflow_engine([row])

        with patch("collector.airflow_collector.create_engine", return_value=mock_engine):
            events = AirflowCollector().collect("sqlite:///fake.db")

        assert len(events) == 1
        e = events[0]
        assert e.model_name == "elt_pipeline.transform_orders"
        assert e.execution_time_ms == pytest.approx(72300.0)
        assert e.status == StatusEnum.success
        assert e.source == SourceEnum.airflow
        assert e.dag_id == "elt_pipeline"
        assert e.run_id == "run_001"

    def test_failed_state_maps_to_error(self):
        row = _make_airflow_row(state="failed")
        mock_engine = _mock_airflow_engine([row])

        with patch("collector.airflow_collector.create_engine", return_value=mock_engine):
            events = AirflowCollector().collect("sqlite:///fake.db")

        assert events[0].status == StatusEnum.error

    def test_none_duration_defaults_to_zero(self):
        row = _make_airflow_row(duration=None)
        mock_engine = _mock_airflow_engine([row])

        with patch("collector.airflow_collector.create_engine", return_value=mock_engine):
            events = AirflowCollector().collect("sqlite:///fake.db")

        assert events[0].execution_time_ms == 0.0

    def test_connection_error_returns_empty(self):
        with patch(
            "collector.airflow_collector.create_engine",
            side_effect=Exception("connection refused"),
        ):
            events = AirflowCollector().collect("postgresql://bad:bad@nowhere/db")

        assert events == []

    def test_multiple_rows(self):
        rows = [_make_airflow_row(task_id=f"task_{i}") for i in range(5)]
        mock_engine = _mock_airflow_engine(rows)

        with patch("collector.airflow_collector.create_engine", return_value=mock_engine):
            events = AirflowCollector().collect("sqlite:///fake.db")

        assert len(events) == 5


# ---------------------------------------------------------------------------
# RedshiftCollector
# ---------------------------------------------------------------------------


def _make_query_row(query=1, label="orders", elapsed_us=5_000_000, rows=800):
    r = MagicMock()
    r.query = query
    r.label = label
    r.total_elapsed_us = elapsed_us
    r.total_rows = rows
    r.start_time = datetime(2026, 4, 1, tzinfo=timezone.utc)
    return r


def _make_load_error_row(tbl="raw_sales"):
    r = MagicMock()
    r.tbl = tbl
    r.starttime = datetime(2026, 4, 1, tzinfo=timezone.utc)
    r.err_reason = "Invalid date format"
    r.filename = "s3://bucket/file.csv"
    return r


def _mock_redshift_engine(query_rows, error_rows):
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [
        MagicMock(fetchall=MagicMock(return_value=query_rows)),
        MagicMock(fetchall=MagicMock(return_value=error_rows)),
    ]
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine


class TestRedshiftCollector:
    def test_basic_collect(self):
        mock_engine = _mock_redshift_engine(
            [_make_query_row()], []
        )
        with patch(
            "collector.redshift_collector.create_engine", return_value=mock_engine
        ):
            events = RedshiftCollector().collect("redshift+psycopg2://fake/db")

        assert len(events) == 1
        e = events[0]
        assert e.model_name == "orders"
        assert e.execution_time_ms == pytest.approx(5000.0)
        assert e.rows_affected == 800
        assert e.status == StatusEnum.success
        assert e.source == SourceEnum.redshift

    def test_load_error_produces_error_event(self):
        mock_engine = _mock_redshift_engine([], [_make_load_error_row()])
        with patch(
            "collector.redshift_collector.create_engine", return_value=mock_engine
        ):
            events = RedshiftCollector().collect("redshift+psycopg2://fake/db")

        assert len(events) == 1
        assert events[0].status == StatusEnum.error
        assert "load_error.raw_sales" == events[0].model_name

    def test_null_label_uses_query_id(self):
        row = _make_query_row(query=42, label=None)
        mock_engine = _mock_redshift_engine([row], [])
        with patch(
            "collector.redshift_collector.create_engine", return_value=mock_engine
        ):
            events = RedshiftCollector().collect("redshift+psycopg2://fake/db")

        assert events[0].model_name == "query_42"

    def test_connection_error_returns_empty(self):
        with patch(
            "collector.redshift_collector.create_engine",
            side_effect=Exception("timeout"),
        ):
            events = RedshiftCollector().collect("redshift+psycopg2://fake/db")

        assert events == []
