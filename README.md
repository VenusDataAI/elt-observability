# ELT Observability Platform

A production-ready Python platform that plugs into any **dbt + Airflow** stack to collect pipeline metrics, detect data quality anomalies, store history in SQLite, generate a self-contained HTML dashboard, and fire structured Slack alerts.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        Collectors                           │
│   DbtCollector  ──  AirflowCollector  ──  RedshiftCollector │
└──────────────────────────┬──────────────────────────────────┘
                           │  list[ObservabilityEvent]
                           ▼
                  ┌─────────────────┐
                  │  MetricsStore   │  (SQLite via SQLAlchemy Core)
                  └────────┬────────┘
                           │
          ┌────────────────┼───────────────────────┐
          ▼                ▼                       ▼
  ┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │  Detectors    │  │  DashboardGen    │  │  SlackAlerter    │
  │  RowCount     │  │  (Jinja2 + SVG)  │  │  (Block Kit)     │
  │  Schema       │  └──────────────────┘  └──────────────────┘
  │  Freshness    │
  │  SilentFail   │
  └───────────────┘
```

### Component Summary

| Component | File | Responsibility |
|---|---|---|
| **Models** | `models.py` | Pydantic v2 data models and enums shared by all modules |
| **Config** | `config.py` | Lazy-loading singleton for `config.yaml` |
| **DbtCollector** | `collector/dbt_collector.py` | Parses `run_results.json` + `manifest.json` |
| **AirflowCollector** | `collector/airflow_collector.py` | Reads Airflow metadata DB via SQLAlchemy |
| **RedshiftCollector** | `collector/redshift_collector.py` | Queries `SVL_QUERY_REPORT` and `STL_LOAD_ERRORS` |
| **MetricsStore** | `storage/metrics_store.py` | SQLite persistence for events and alerts |
| **RowCountDrift** | `detectors/row_count_drift.py` | Z-score drift detection over rolling window |
| **SchemaDrift** | `detectors/schema_drift.py` | Column-level diff against a stored baseline |
| **FreshnessDetector** | `detectors/freshness_detector.py` | SLA breach detection by model age |
| **SilentFailure** | `detectors/silent_failure_detector.py` | Detects success + 0-row runs vs. historical average |
| **DashboardGenerator** | `dashboards/dashboard_generator.py` | Renders self-contained HTML with SVG charts |
| **SlackAlerter** | `alerting/slack_alerter.py` | Posts Block Kit messages with 1-hour deduplication |

---

## Quickstart

### 1. Install

```bash
pip install -e ".[dev]"
```

Or with uv:

```bash
uv pip install -e ".[dev]"
```

### 2. Configure

Edit `config.yaml`:

```yaml
metrics_store_path: ./output/metrics.db
freshness_sla:
  bronze: 2   # hours
  silver: 4
  gold: 8
drift_z_threshold: 2.5
drift_window_runs: 10
slack_webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
base_url: "http://your-platform:8080"
```

### 3. Point at dbt artefacts

```python
from collector import DbtCollector
from storage import MetricsStore
from detectors import RowCountDriftDetector
from dashboards.dashboard_generator import DashboardGenerator

store = MetricsStore("./output/metrics.db")
collector = DbtCollector()

events = collector.collect(
    run_results_path="target/run_results.json",
    manifest_path="target/manifest.json",
)

for event in events:
    store.save_event(event)

    history = store.get_model_history(event.model_name, last_n=10)
    alert = RowCountDriftDetector().detect(event.model_name, event, history)
    if alert:
        store.save_alert(alert)

open_alerts = store.get_open_alerts()
DashboardGenerator().generate(events, open_alerts, "output/dashboard.html")
```

Open `output/dashboard.html` in any browser; no server required.

---

## Configuration Reference

| Key | Type | Description |
|---|---|---|
| `metrics_store_path` | `str` | Path to the SQLite database file |
| `freshness_sla.bronze` | `int` | Max age in hours for bronze-layer models |
| `freshness_sla.silver` | `int` | Max age in hours for silver-layer models |
| `freshness_sla.gold` | `int` | Max age in hours for gold-layer models |
| `drift_z_threshold` | `float` | Z-score threshold to trigger a drift alert (default 2.5) |
| `drift_window_runs` | `int` | Number of historical runs to include in the rolling window |
| `slack_webhook_url` | `str` | Incoming Webhook URL (leave empty to disable Slack) |
| `base_url` | `str` | Dashboard base URL embedded in Slack alert links |

---

## Slack Setup

1. In your Slack workspace: **Apps → Incoming Webhooks → Add to Slack**.
2. Choose a channel and copy the Webhook URL.
3. Paste it into `config.yaml` under `slack_webhook_url`.
4. Set `base_url` to wherever your `dashboard.html` is served.

Alerts fire once per `alert_id` per hour (in-memory deduplication). The cache resets on process restart.

---

## dbt Integration

### Post-hook (warehouse logging)

Add to `dbt_project.yml` to log every model run to `_observability_log`:

```yaml
models:
  your_project:
    +post-hook: "{{ observability_post_hook() }}"
```

Or on a single model:

```sql
{{ config(post_hook="{{ observability_post_hook() }}") }}
```

### Zero-row audit analysis

```bash
dbt compile --select row_count_audit
```

The compiled SQL surfaces models with zero-row runs in the last 7 days from `_observability_log`.

---

## Detectors In Depth

### RowCountDriftDetector

Uses a rolling z-score over the last `drift_window_runs` runs:

```
z = |current_rows - mean(history)| / stdev(history)
```

- `z > threshold` → **WARNING**
- `z > threshold × 2` → **CRITICAL**
- `< 2` historical runs → no alert (insufficient data)
- `stdev == 0` and current differs → **WARNING**

### SchemaDriftDetector

Compares the `columns` dict in the current `manifest.json` against a stored baseline snapshot:

- Column added → **WARNING**
- Column removed → **WARNING**
- Column type changed → **CRITICAL**

Pass the baseline as `nodes["model.project.name"]["columns"]` from a previously saved manifest.

### FreshnessDetector

```
age_hours = (now - last_successful_run) / 3600
```

- `age_hours > sla_hours` → **WARNING**
- `age_hours > sla_hours × 2` → **CRITICAL**

SLA hours are looked up from `config.yaml` by layer (`bronze`/`silver`/`gold`).

### SilentFailureDetector

Triggers when:
- `status == "success"` AND
- `rows_affected == 0` AND
- `mean(historical_rows) > min_expected_rows` (default 100)

Severity is always **CRITICAL**, as silent failures are the most dangerous class of data quality incident.

---

## Adding a New Detector

1. Create `detectors/my_detector.py` with a class implementing:
   ```python
   def detect(self, model_name: str, current_event: ObservabilityEvent, ...) -> DriftAlert | None:
   ```
2. Return `None` when healthy, a `DriftAlert` when a problem is found.
3. Export from `detectors/__init__.py`.
4. Add test cases in `tests/test_detectors.py`.
5. Wire it into your collection loop alongside the existing detectors.

---

## Running Tests

```bash
pytest                          # all tests
pytest --cov=. --cov-report=term-missing   # with coverage
pytest tests/test_detectors.py  # single file
```

Current coverage: **99%** across 72 tests.

---

## Project Structure

```
elt-observability/
├── collector/
│   ├── dbt_collector.py        # Parses dbt artefacts
│   ├── airflow_collector.py    # Reads Airflow metadata DB
│   └── redshift_collector.py   # SVL_QUERY_REPORT / STL_LOAD_ERRORS
├── detectors/
│   ├── row_count_drift.py      # Z-score drift detection
│   ├── schema_drift.py         # Column-level diff
│   ├── freshness_detector.py   # SLA age check
│   └── silent_failure_detector.py
├── storage/
│   └── metrics_store.py        # SQLite via SQLAlchemy Core
├── dashboards/
│   ├── sla_dashboard.html      # Jinja2 template (no CDN)
│   └── dashboard_generator.py
├── alerting/
│   └── slack_alerter.py        # Block Kit + 1h dedup
├── dbt_integration/
│   ├── macros/observability_hooks.sql
│   └── analyses/row_count_audit.sql
├── tests/
│   ├── test_collectors.py
│   ├── test_detectors.py
│   ├── test_alerting.py
│   └── test_storage.py
├── output/                     # Generated dashboard.html + metrics.db
├── models.py                   # Pydantic v2 shared models
├── config.py                   # Lazy config singleton
├── config.yaml                 # Runtime configuration
└── pyproject.toml
```

---

## Tech Stack

- **Python 3.11+** · **Pydantic v2** · **SQLAlchemy 2.0** (Core only)
- **Jinja2** (dashboard templating) · **httpx** (Slack HTTP)
- **structlog** (structured logging) · **pandas** (available for extensions)
- **pytest** + **pytest-cov** (testing)
