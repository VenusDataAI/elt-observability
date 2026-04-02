"""Collector that reads Airflow's metadata DB via SQLAlchemy."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from sqlalchemy import create_engine, text

from models import ObservabilityEvent, SourceEnum, StatusEnum

log = structlog.get_logger(__name__)

_TASK_QUERY = text(
    """
    SELECT
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.start_date,
        ti.end_date,
        ti.state,
        ti.duration
    FROM task_instance ti
    JOIN dag_run dr ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
    WHERE ti.start_date >= :since
    ORDER BY ti.start_date DESC
    LIMIT :limit
    """
)

_STATUS_MAP: dict[str, StatusEnum] = {
    "success": StatusEnum.success,
    "failed": StatusEnum.error,
    "skipped": StatusEnum.skipped,
    "upstream_failed": StatusEnum.error,
}


class AirflowCollector:
    """Extract task-level ObservabilityEvents from an Airflow metadata DB."""

    def collect(
        self,
        connection_string: str,
        hours_back: int = 24,
        limit: int = 1000,
    ) -> list[ObservabilityEvent]:
        try:
            engine = create_engine(connection_string, future=True)
            since = datetime.now(timezone.utc) - timedelta(hours=hours_back)
            with engine.connect() as conn:
                rows = conn.execute(
                    _TASK_QUERY,
                    {"since": since, "limit": limit},
                ).fetchall()
        except Exception as exc:
            log.error("airflow_collect_failed", error=str(exc))
            return []

        events: list[ObservabilityEvent] = []
        for row in rows:
            status = _STATUS_MAP.get(row.state or "", StatusEnum.error)
            events.append(
                ObservabilityEvent(
                    model_name=f"{row.dag_id}.{row.task_id}",
                    invocation_id=row.run_id or "unknown",
                    execution_time_ms=float(row.duration or 0) * 1000,
                    rows_affected=0,
                    status=status,
                    source=SourceEnum.airflow,
                    dag_id=row.dag_id,
                    run_id=row.run_id,
                    timestamp=_ensure_utc(row.start_date),
                )
            )
        return events


def _ensure_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
