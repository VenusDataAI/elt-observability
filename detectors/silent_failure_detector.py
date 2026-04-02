"""Detects models that succeed with zero rows when historically non-empty."""

from __future__ import annotations

import statistics

import structlog

from models import AlertType, DriftAlert, ObservabilityEvent, Severity, StatusEnum

log = structlog.get_logger(__name__)


class SilentFailureDetector:
    """Detect success+0-row runs when the historical average is above a threshold."""

    def detect(
        self,
        model_name: str,
        current_event: ObservabilityEvent,
        history: list[ObservabilityEvent],
        min_expected_rows: int = 100,
    ) -> DriftAlert | None:
        if current_event.status != StatusEnum.success:
            return None
        if current_event.rows_affected != 0:
            return None
        if len(history) < 2:
            return None

        mean = statistics.mean([e.rows_affected for e in history])
        if mean <= min_expected_rows:
            return None

        return DriftAlert(
            model_name=model_name,
            alert_type=AlertType.SILENT_FAILURE,
            severity=Severity.CRITICAL,
            description=(
                f"Model '{model_name}' reported success with 0 rows "
                f"(historical average: {mean:.0f} rows)"
            ),
            current_value=0.0,
            expected_value=mean,
        )
