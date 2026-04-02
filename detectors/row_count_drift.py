"""Detects row-count drift using a rolling z-score."""

from __future__ import annotations

import statistics

import structlog

from models import AlertType, DriftAlert, ObservabilityEvent, Severity

log = structlog.get_logger(__name__)


class RowCountDriftDetector:
    """Flag models whose row count deviates significantly from historical norms."""

    def detect(
        self,
        model_name: str,
        current_event: ObservabilityEvent,
        history: list[ObservabilityEvent],
        window: int = 10,
        z_threshold: float = 2.5,
    ) -> DriftAlert | None:
        sample = history[-window:]
        counts = [e.rows_affected for e in sample]

        if len(counts) < 2:
            log.debug("drift_insufficient_history", model=model_name, n=len(counts))
            return None

        mean = statistics.mean(counts)
        current = float(current_event.rows_affected)

        try:
            stddev = statistics.stdev(counts)
        except statistics.StatisticsError:
            stddev = 0.0

        if stddev == 0.0:
            if current == mean:
                return None
            # Any deviation from a perfectly constant series is notable
            return DriftAlert(
                model_name=model_name,
                alert_type=AlertType.ROW_COUNT_DRIFT,
                severity=Severity.WARNING,
                description=(
                    f"Row count {int(current)} deviates from constant historical "
                    f"value {mean:.0f} (stddev=0, window={len(counts)})"
                ),
                current_value=current,
                expected_value=mean,
            )

        z = abs(current - mean) / stddev
        if z <= z_threshold:
            return None

        severity = Severity.CRITICAL if z > z_threshold * 2 else Severity.WARNING
        return DriftAlert(
            model_name=model_name,
            alert_type=AlertType.ROW_COUNT_DRIFT,
            severity=severity,
            description=(
                f"Row count {int(current)} deviates {z:.2f} std devs from "
                f"mean {mean:.0f} (window={len(counts)})"
            ),
            current_value=current,
            expected_value=mean,
        )
