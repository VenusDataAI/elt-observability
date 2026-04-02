"""Shared Pydantic models and enums for the ELT Observability Platform."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class StatusEnum(str, Enum):
    success = "success"
    error = "error"
    skipped = "skipped"
    warn = "warn"


class SourceEnum(str, Enum):
    dbt = "dbt"
    airflow = "airflow"
    redshift = "redshift"


class AlertType(str, Enum):
    ROW_COUNT_DRIFT = "ROW_COUNT_DRIFT"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    FRESHNESS = "FRESHNESS"
    SILENT_FAILURE = "SILENT_FAILURE"


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class ChangeType(str, Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    TYPE_CHANGED = "TYPE_CHANGED"


# ---------------------------------------------------------------------------
# Core event model
# ---------------------------------------------------------------------------


class ObservabilityEvent(BaseModel):
    model_name: str
    invocation_id: str
    execution_time_ms: float
    rows_affected: int
    status: StatusEnum
    compiled_sql_hash: Optional[str] = None
    source: SourceEnum
    timestamp: datetime = Field(default_factory=_utcnow)
    dag_id: Optional[str] = None
    run_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Alert models
# ---------------------------------------------------------------------------


class DriftAlert(BaseModel):
    alert_id: UUID = Field(default_factory=uuid4)
    model_name: str
    alert_type: AlertType
    severity: Severity
    description: str
    current_value: float
    expected_value: float
    created_at: datetime = Field(default_factory=_utcnow)
    resolved_at: Optional[datetime] = None


class FreshnessAlert(DriftAlert):
    alert_type: AlertType = AlertType.FRESHNESS
    layer: str
    sla_hours: float
    actual_age_hours: float


# ---------------------------------------------------------------------------
# Schema change model
# ---------------------------------------------------------------------------


class SchemaChange(BaseModel):
    column_name: str
    change_type: ChangeType
    old_type: Optional[str] = None
    new_type: Optional[str] = None
