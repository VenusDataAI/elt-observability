"""Collector that queries Redshift SVL_QUERY_REPORT and STL_LOAD_ERRORS."""

from __future__ import annotations

import structlog
from sqlalchemy import create_engine, text

from models import ObservabilityEvent, SourceEnum, StatusEnum

log = structlog.get_logger(__name__)

_QUERY_REPORT_SQL = text(
    """
    SELECT
        query,
        label,
        SUM(elapsed)    AS total_elapsed_us,
        SUM(rows)       AS total_rows,
        MIN(start_time) AS start_time
    FROM SVL_QUERY_REPORT
    WHERE start_time >= DATEADD(hour, -:hours_back, GETDATE())
    GROUP BY query, label
    ORDER BY start_time DESC
    LIMIT 1000
    """
)

_LOAD_ERRORS_SQL = text(
    """
    SELECT
        tbl,
        starttime,
        err_reason,
        filename
    FROM STL_LOAD_ERRORS
    WHERE starttime >= DATEADD(hour, -:hours_back, GETDATE())
    ORDER BY starttime DESC
    """
)


class RedshiftCollector:
    """Extract query execution stats and load errors from Redshift."""

    def collect(
        self,
        connection_string: str,
        hours_back: int = 24,
    ) -> list[ObservabilityEvent]:
        try:
            engine = create_engine(connection_string, future=True)
            params = {"hours_back": hours_back}
            with engine.connect() as conn:
                query_rows = conn.execute(_QUERY_REPORT_SQL, params).fetchall()
                error_rows = conn.execute(_LOAD_ERRORS_SQL, params).fetchall()
        except Exception as exc:
            log.error("redshift_collect_failed", error=str(exc))
            return []

        events: list[ObservabilityEvent] = []

        for row in query_rows:
            model_name = row.label or f"query_{row.query}"
            events.append(
                ObservabilityEvent(
                    model_name=model_name,
                    invocation_id=str(row.query),
                    execution_time_ms=float(row.total_elapsed_us or 0) / 1000.0,
                    rows_affected=int(row.total_rows or 0),
                    status=StatusEnum.success,
                    source=SourceEnum.redshift,
                )
            )

        for row in error_rows:
            events.append(
                ObservabilityEvent(
                    model_name=f"load_error.{row.tbl}",
                    invocation_id="load_error",
                    execution_time_ms=0.0,
                    rows_affected=0,
                    status=StatusEnum.error,
                    source=SourceEnum.redshift,
                    compiled_sql_hash=None,
                )
            )

        return events
