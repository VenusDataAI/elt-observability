"""Detects stale models by comparing last run time against SLA thresholds."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from models import AlertType, FreshnessAlert, Severity

log = structlog.get_logger(__name__)


class FreshnessDetector:
    """Alert when a model's last successful run is older than its SLA."""

    def detect(
        self,
        model_name: str,
        last_run_time: datetime,
        sla_hours: float,
        layer: str = "bronze",
    ) -> FreshnessAlert | None:
        # Ensure timezone-aware
        if last_run_time.tzinfo is None:
            last_run_time = last_run_time.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        age_hours = (now - last_run_time).total_seconds() / 3600.0

        if age_hours <= sla_hours:
            return None

        severity = (
            Severity.CRITICAL if age_hours > sla_hours * 2 else Severity.WARNING
        )

        return FreshnessAlert(
            model_name=model_name,
            alert_type=AlertType.FRESHNESS,
            severity=severity,
            description=(
                f"Model '{model_name}' is {age_hours:.1f}h old, "
                f"exceeds SLA of {sla_hours}h ({layer} layer)"
            ),
            current_value=age_hours,
            expected_value=sla_hours,
            layer=layer,
            sla_hours=sla_hours,
            actual_age_hours=age_hours,
        )
