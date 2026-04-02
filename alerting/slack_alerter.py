"""Slack alerter with in-memory deduplication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import structlog

from models import DriftAlert, Severity

log = structlog.get_logger(__name__)

# Module-level dedup cache: alert_id → last_sent_at
_dedup_cache: dict[str, datetime] = {}
_DEDUP_TTL = timedelta(hours=1)

_SEVERITY_EMOJI: dict[Severity, str] = {
    Severity.INFO: ":information_source:",
    Severity.WARNING: ":warning:",
    Severity.CRITICAL: ":rotating_light:",
}


class SlackAlerter:
    """Post structured Slack Block Kit messages for DriftAlerts."""

    def __init__(
        self,
        webhook_url: str,
        base_url: str = "http://localhost:8080",
    ) -> None:
        self.webhook_url = webhook_url
        self.base_url = base_url

    def send_alert(self, alert: DriftAlert) -> bool:
        """
        Post the alert to Slack.

        Returns True on success or deduplicated skip; False on send failure.
        """
        alert_id_str = str(alert.alert_id)
        now = datetime.now(timezone.utc)

        # Deduplication check
        last_sent = _dedup_cache.get(alert_id_str)
        if last_sent is not None and (now - last_sent) < _DEDUP_TTL:
            log.debug("slack_alert_deduplicated", alert_id=alert_id_str)
            return True

        payload = self._build_payload(alert)

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(self.webhook_url, json=payload)
            if response.status_code != 200:
                log.error(
                    "slack_send_failed",
                    status=response.status_code,
                    body=response.text,
                )
                return False
        except httpx.HTTPError as exc:
            log.error("slack_http_error", error=str(exc))
            return False

        _dedup_cache[alert_id_str] = now
        log.info("slack_alert_sent", model=alert.model_name, type=alert.alert_type.value)
        return True

    # ------------------------------------------------------------------

    def _build_payload(self, alert: DriftAlert) -> dict[str, Any]:
        emoji = _SEVERITY_EMOJI.get(alert.severity, ":bell:")
        dashboard_url = f"{self.base_url}/dashboard.html"

        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} ELT Alert: {alert.alert_type.value}",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Model:*\n`{alert.model_name}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:*\n{alert.severity.value}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Current Value:*\n{alert.current_value:.2f}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Expected Value:*\n{alert.expected_value:.2f}",
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Description:*\n{alert.description}",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"Alert ID: `{alert.alert_id}` | "
                                f"<{dashboard_url}|View Dashboard>"
                            ),
                        }
                    ],
                },
            ]
        }
