"""Collector that parses dbt run_results.json and manifest.json."""

from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any

import structlog

from models import ObservabilityEvent, SourceEnum, StatusEnum

log = structlog.get_logger(__name__)

_STATUS_MAP: dict[str, StatusEnum] = {
    "success": StatusEnum.success,
    "error": StatusEnum.error,
    "skipped": StatusEnum.skipped,
    "warn": StatusEnum.warn,
}


class DbtCollector:
    """Parse dbt artefact files into ObservabilityEvents."""

    def collect(
        self,
        run_results_path: str,
        manifest_path: str,
    ) -> list[ObservabilityEvent]:
        run_results = self._load_json(run_results_path)
        if run_results is None:
            return []
        manifest = self._load_json(manifest_path)  # optional – may be None
        return self._parse_results(run_results, manifest)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_json(self, path: str) -> dict[str, Any] | None:
        try:
            return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            log.warning("dbt_file_not_found", path=path)
            return None
        except json.JSONDecodeError as exc:
            log.warning("dbt_json_parse_error", path=path, error=str(exc))
            return None

    @staticmethod
    def _hash_sql(sql: str) -> str:
        return hashlib.sha256(sql.encode()).hexdigest()[:16]

    def _parse_results(
        self,
        run_results: dict[str, Any],
        manifest: dict[str, Any] | None,
    ) -> list[ObservabilityEvent]:
        invocation_id: str = (
            run_results.get("metadata", {}).get("invocation_id", "unknown")
        )
        nodes: dict[str, Any] = (manifest or {}).get("nodes", {})
        events: list[ObservabilityEvent] = []

        for result in run_results.get("results", []):
            unique_id: str = result.get("unique_id", "")
            model_name: str = unique_id.split(".")[-1] if unique_id else "unknown"

            raw_status = result.get("status", "error")
            status = _STATUS_MAP.get(raw_status, StatusEnum.error)

            execution_time_ms = float(result.get("execution_time", 0)) * 1000

            adapter_response = result.get("adapter_response") or {}
            rows_affected = int(adapter_response.get("rows_affected") or 0)

            node = nodes.get(unique_id, {})
            compiled_sql: str = node.get("compiled_sql") or node.get("compiled_code") or ""
            sql_hash = self._hash_sql(compiled_sql) if compiled_sql else None

            events.append(
                ObservabilityEvent(
                    model_name=model_name,
                    invocation_id=invocation_id,
                    execution_time_ms=execution_time_ms,
                    rows_affected=rows_affected,
                    status=status,
                    compiled_sql_hash=sql_hash,
                    source=SourceEnum.dbt,
                )
            )

        return events
