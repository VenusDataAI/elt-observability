"""Detects schema drift by comparing manifest column lists against a baseline."""

from __future__ import annotations

import json
import pathlib
from typing import Any

import structlog

from models import AlertType, ChangeType, DriftAlert, SchemaChange, Severity

log = structlog.get_logger(__name__)


class SchemaDriftDetector:
    """Compare the current dbt manifest column list against a stored baseline."""

    def detect(
        self,
        model_name: str,
        current_manifest_path: str,
        baseline_snapshot: dict[str, Any],
    ) -> list[DriftAlert]:
        """
        Return one DriftAlert per changed column.

        *baseline_snapshot* is the ``nodes[model_name]["columns"]`` dict from a
        previously stored manifest (column_name → {"data_type": ...}).
        """
        current_columns = self._load_columns(current_manifest_path, model_name)
        if current_columns is None:
            log.warning("schema_drift_manifest_missing", model=model_name)
            return []

        changes = _diff_columns(baseline_snapshot, current_columns)
        alerts: list[DriftAlert] = []
        for change in changes:
            severity = (
                Severity.CRITICAL
                if change.change_type == ChangeType.TYPE_CHANGED
                else Severity.WARNING
            )
            description = _describe_change(change)
            alerts.append(
                DriftAlert(
                    model_name=model_name,
                    alert_type=AlertType.SCHEMA_DRIFT,
                    severity=severity,
                    description=description,
                    current_value=1.0,
                    expected_value=0.0,
                )
            )
        return alerts

    # ------------------------------------------------------------------

    def _load_columns(
        self, manifest_path: str, model_name: str
    ) -> dict[str, Any] | None:
        try:
            data = json.loads(
                pathlib.Path(manifest_path).read_text(encoding="utf-8")
            )
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            log.warning("schema_manifest_load_error", path=manifest_path, error=str(exc))
            return None

        nodes = data.get("nodes", {})
        # Support both "model.project.name" key and plain "name" lookup
        for key, node in nodes.items():
            if key.endswith(f".{model_name}") or node.get("name") == model_name:
                return node.get("columns", {})
        log.warning("schema_model_not_in_manifest", model=model_name)
        return {}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _diff_columns(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[SchemaChange]:
    changes: list[SchemaChange] = []

    for col, meta in current.items():
        if col not in baseline:
            changes.append(SchemaChange(column_name=col, change_type=ChangeType.ADDED))
        else:
            old_type = (baseline[col] or {}).get("data_type")
            new_type = (meta or {}).get("data_type")
            if old_type != new_type:
                changes.append(
                    SchemaChange(
                        column_name=col,
                        change_type=ChangeType.TYPE_CHANGED,
                        old_type=old_type,
                        new_type=new_type,
                    )
                )

    for col in baseline:
        if col not in current:
            changes.append(SchemaChange(column_name=col, change_type=ChangeType.REMOVED))

    return changes


def _describe_change(change: SchemaChange) -> str:
    if change.change_type == ChangeType.ADDED:
        return f"Column '{change.column_name}' was ADDED"
    if change.change_type == ChangeType.REMOVED:
        return f"Column '{change.column_name}' was REMOVED"
    return (
        f"Column '{change.column_name}' type changed: "
        f"{change.old_type} → {change.new_type}"
    )
