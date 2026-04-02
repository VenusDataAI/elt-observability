"""Generates a self-contained HTML observability dashboard."""

from __future__ import annotations

import pathlib
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from jinja2 import Environment, FileSystemLoader

from models import DriftAlert, ObservabilityEvent

log = structlog.get_logger(__name__)

TEMPLATE_DIR = pathlib.Path(__file__).parent
TEMPLATE_NAME = "sla_dashboard.html"

# SVG chart dimensions
_SVG_W = 500
_SVG_H = 120
_PAD_LEFT = 50
_PAD_RIGHT = 15
_PAD_TOP = 15
_PAD_BOTTOM = 25


class DashboardGenerator:
    """Build and write a fully self-contained HTML dashboard."""

    def generate(
        self,
        events: list[ObservabilityEvent],
        alerts: list[DriftAlert],
        output_path: str = "output/dashboard.html",
    ) -> str:
        """Render the dashboard and write it to *output_path*. Returns the path."""
        out = pathlib.Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        context = _build_context(events, alerts)
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=False,
        )
        template = env.get_template(TEMPLATE_NAME)
        html = template.render(**context)
        out.write_text(html, encoding="utf-8")
        log.info("dashboard_written", path=str(out))
        return str(out)


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------


def _build_context(
    events: list[ObservabilityEvent],
    alerts: list[DriftAlert],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    recent = [e for e in events if _ensure_utc(e.timestamp) >= cutoff]

    # --- health summary ---
    model_names = {e.model_name for e in recent}
    warning_count = sum(1 for a in alerts if a.severity.value == "WARNING")
    critical_count = sum(1 for a in alerts if a.severity.value == "CRITICAL")

    health = {
        "total_models": len(model_names),
        "total_events": len(recent),
        "warning_count": warning_count,
        "critical_count": critical_count,
    }

    # --- per-model latest event ---
    latest: dict[str, ObservabilityEvent] = {}
    for e in recent:
        if e.model_name not in latest or _ensure_utc(e.timestamp) > _ensure_utc(latest[e.model_name].timestamp):
            latest[e.model_name] = e

    open_alert_models = {a.model_name for a in alerts if a.resolved_at is None}

    model_rows = []
    for name, evt in sorted(latest.items()):
        sla_status = "critical" if name in open_alert_models else "ok"
        model_rows.append({
            "name": name,
            "last_run": _ensure_utc(evt.timestamp).strftime("%Y-%m-%d %H:%M UTC"),
            "status": evt.status.value,
            "rows": evt.rows_affected,
            "exec_ms": evt.execution_time_ms,
            "sla_status": sla_status,
        })

    # --- top-5 slowest models SVG charts ---
    # Aggregate all events per model → list of exec times (chronological)
    model_times: dict[str, list[float]] = {}
    for e in sorted(events, key=lambda x: _ensure_utc(x.timestamp)):
        model_times.setdefault(e.model_name, []).append(e.execution_time_ms)

    slowest = sorted(
        model_times.items(),
        key=lambda kv: max(kv[1]),
        reverse=True,
    )[:5]

    charts = [
        {
            "model_name": name,
            "svg_markup": _build_svg_chart(name, times),
        }
        for name, times in slowest
        if len(times) >= 2
    ]

    # --- alert rows ---
    alert_rows = [
        {
            "model_name": a.model_name,
            "alert_type": a.alert_type.value,
            "severity": a.severity.value,
            "description": a.description,
            "created_at": _ensure_utc(a.created_at).strftime("%Y-%m-%d %H:%M UTC"),
        }
        for a in alerts
        if a.resolved_at is None
    ]

    return {
        "title": "ELT Observability Dashboard",
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        "health": health,
        "models": model_rows,
        "charts": charts,
        "alerts": alert_rows,
    }


# ---------------------------------------------------------------------------
# SVG chart
# ---------------------------------------------------------------------------


def _build_svg_chart(model_name: str, exec_times_ms: list[float]) -> str:
    """Return a pure-SVG line chart for the given execution times."""
    times_s = [t / 1000.0 for t in exec_times_ms]
    n = len(times_s)
    if n < 2:
        return ""

    min_v = min(times_s)
    max_v = max(times_s)
    span = max_v - min_v if max_v != min_v else 1.0

    plot_w = _SVG_W - _PAD_LEFT - _PAD_RIGHT
    plot_h = _SVG_H - _PAD_TOP - _PAD_BOTTOM

    def x_pos(i: int) -> float:
        return _PAD_LEFT + (i / (n - 1)) * plot_w

    def y_pos(v: float) -> float:
        return _PAD_TOP + plot_h - ((v - min_v) / span) * plot_h

    points = " ".join(f"{x_pos(i):.1f},{y_pos(v):.1f}" for i, v in enumerate(times_s))

    # Axis tick labels
    y_labels = [
        f'<text x="{_PAD_LEFT - 4}" y="{_PAD_TOP + plot_h:.1f}" '
        f'font-size="9" fill="#888" text-anchor="end">{min_v:.2f}s</text>',
        f'<text x="{_PAD_LEFT - 4}" y="{_PAD_TOP + 4:.1f}" '
        f'font-size="9" fill="#888" text-anchor="end">{max_v:.2f}s</text>',
    ]

    # Circles at each data point
    circles = [
        f'<circle cx="{x_pos(i):.1f}" cy="{y_pos(v):.1f}" r="3" '
        f'fill="#4a90d9" stroke="#fff" stroke-width="1.5"/>'
        for i, v in enumerate(times_s)
    ]

    # Horizontal guide lines
    guides = [
        f'<line x1="{_PAD_LEFT}" y1="{_PAD_TOP:.1f}" '
        f'x2="{_SVG_W - _PAD_RIGHT}" y2="{_PAD_TOP:.1f}" '
        f'stroke="#eee" stroke-width="1"/>',
        f'<line x1="{_PAD_LEFT}" y1="{_PAD_TOP + plot_h:.1f}" '
        f'x2="{_SVG_W - _PAD_RIGHT}" y2="{_PAD_TOP + plot_h:.1f}" '
        f'stroke="#ddd" stroke-width="1"/>',
    ]

    svg = (
        f'<svg width="{_SVG_W}" height="{_SVG_H}" xmlns="http://www.w3.org/2000/svg">'
        + "".join(guides)
        + f'<polyline points="{points}" fill="none" stroke="#4a90d9" stroke-width="2" stroke-linejoin="round"/>'
        + "".join(circles)
        + "".join(y_labels)
        + "</svg>"
    )
    return svg


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
