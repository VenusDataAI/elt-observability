"""SQLite-backed metrics store using SQLAlchemy Core."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    select,
    update,
)

from models import AlertType, DriftAlert, FreshnessAlert, ObservabilityEvent

log = structlog.get_logger(__name__)


class MetricsStore:
    """Persist ObservabilityEvents and DriftAlerts in SQLite."""

    def __init__(self, db_path: str) -> None:
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(f"sqlite:///{db_path}", future=True)
        self._meta = MetaData()
        self._define_tables()
        self._meta.create_all(self.engine)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _define_tables(self) -> None:
        self._events = Table(
            "observability_events",
            self._meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("model_name", String, nullable=False),
            Column("invocation_id", String, nullable=False),
            Column("execution_time_ms", Float),
            Column("rows_affected", Integer),
            Column("status", String),
            Column("compiled_sql_hash", String),
            Column("source", String),
            Column("timestamp", String),
            Column("dag_id", String),
            Column("run_id", String),
        )

        self._alerts = Table(
            "alerts",
            self._meta,
            Column("alert_id", String, primary_key=True),
            Column("model_name", String, nullable=False),
            Column("alert_type", String),
            Column("severity", String),
            Column("description", Text),
            Column("current_value", Float),
            Column("expected_value", Float),
            Column("created_at", String),
            Column("resolved_at", String),
            # FreshnessAlert extras (nullable for other alert types)
            Column("layer", String),
            Column("sla_hours", Float),
            Column("actual_age_hours", Float),
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def save_event(self, event: ObservabilityEvent) -> None:
        row = event.model_dump()
        row["status"] = event.status.value
        row["source"] = event.source.value
        row["timestamp"] = event.timestamp.isoformat()
        with self.engine.begin() as conn:
            conn.execute(self._events.insert(), row)
        log.debug("event_saved", model=event.model_name)

    def get_model_history(
        self, model_name: str, last_n: int = 10
    ) -> list[ObservabilityEvent]:
        stmt = (
            select(self._events)
            .where(self._events.c.model_name == model_name)
            .order_by(self._events.c.id.desc())
            .limit(last_n)
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        events: list[ObservabilityEvent] = []
        for row in reversed(rows):  # chronological order
            d = dict(row._mapping)
            d.pop("id", None)
            events.append(ObservabilityEvent(**d))
        return events

    # ------------------------------------------------------------------
    # Alerts
    # ------------------------------------------------------------------

    def save_alert(self, alert: DriftAlert) -> None:
        row: dict[str, Any] = {
            "alert_id": str(alert.alert_id),
            "model_name": alert.model_name,
            "alert_type": alert.alert_type.value,
            "severity": alert.severity.value,
            "description": alert.description,
            "current_value": alert.current_value,
            "expected_value": alert.expected_value,
            "created_at": alert.created_at.isoformat(),
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
            "layer": None,
            "sla_hours": None,
            "actual_age_hours": None,
        }
        if isinstance(alert, FreshnessAlert):
            row["layer"] = alert.layer
            row["sla_hours"] = alert.sla_hours
            row["actual_age_hours"] = alert.actual_age_hours

        with self.engine.begin() as conn:
            conn.execute(self._alerts.insert(), row)
        log.debug("alert_saved", model=alert.model_name, type=alert.alert_type.value)

    def get_open_alerts(self) -> list[DriftAlert]:
        stmt = select(self._alerts).where(self._alerts.c.resolved_at == None)  # noqa: E711
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        alerts: list[DriftAlert] = []
        for row in rows:
            d = dict(row._mapping)
            alerts.append(_row_to_alert(d))
        return alerts

    def resolve_alert(self, alert_id: str) -> bool:
        stmt = (
            update(self._alerts)
            .where(self._alerts.c.alert_id == alert_id)
            .values(resolved_at=datetime.now(timezone.utc).isoformat())
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
        resolved = result.rowcount > 0
        if resolved:
            log.info("alert_resolved", alert_id=alert_id)
        return resolved


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _row_to_alert(d: dict[str, Any]) -> DriftAlert:
    """Reconstruct a DriftAlert (or FreshnessAlert) from a DB row dict."""
    alert_type = d.get("alert_type")

    kwargs: dict[str, Any] = {
        "alert_id": UUID(d["alert_id"]),
        "model_name": d["model_name"],
        "alert_type": alert_type,
        "severity": d["severity"],
        "description": d["description"] or "",
        "current_value": d["current_value"] or 0.0,
        "expected_value": d["expected_value"] or 0.0,
        "created_at": datetime.fromisoformat(d["created_at"]),
        "resolved_at": datetime.fromisoformat(d["resolved_at"]) if d.get("resolved_at") else None,
    }

    if alert_type == AlertType.FRESHNESS.value:
        return FreshnessAlert(
            **kwargs,
            layer=d.get("layer") or "bronze",
            sla_hours=d.get("sla_hours") or 0.0,
            actual_age_hours=d.get("actual_age_hours") or 0.0,
        )
    return DriftAlert(**kwargs)
