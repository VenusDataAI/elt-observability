-- =============================================================================
-- row_count_audit.sql
-- Surfaces models with zero-row runs in the last 7 days from the
-- _observability_log table written by the observability_post_hook macro.
--
-- Compile with:
--   dbt compile --select row_count_audit
--
-- Run directly:
--   dbt run-operation run_query --args '{query: "$(cat analyses/row_count_audit.sql)"}'
--
-- Or materialise as a view/table:
--   dbt run --select row_count_audit
-- =============================================================================

WITH recent_runs AS (
    SELECT
        model_name,
        invocation_id,
        run_started_at,
        status,
        rows_affected,
        logged_at,
        ROW_NUMBER() OVER (
            PARTITION BY model_name
            ORDER BY logged_at DESC
        ) AS run_rank
    FROM {{ target.schema }}._observability_log
    WHERE
        logged_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        AND status = 'success'
),

zero_row_runs AS (
    SELECT
        model_name,
        COUNT(*)                                    AS zero_row_run_count,
        MIN(logged_at)                              AS first_zero_row_at,
        MAX(logged_at)                              AS last_zero_row_at,
        SUM(CASE WHEN run_rank = 1 THEN 1 ELSE 0 END) AS is_latest_run_zero
    FROM recent_runs
    WHERE rows_affected = 0
    GROUP BY model_name
),

model_totals AS (
    SELECT
        model_name,
        COUNT(*)    AS total_runs,
        AVG(rows_affected::FLOAT) AS avg_rows
    FROM recent_runs
    GROUP BY model_name
)

SELECT
    z.model_name,
    z.zero_row_run_count,
    m.total_runs,
    ROUND(100.0 * z.zero_row_run_count / NULLIF(m.total_runs, 0), 1) AS zero_row_pct,
    ROUND(m.avg_rows, 0)                                               AS avg_rows_last_7d,
    z.first_zero_row_at,
    z.last_zero_row_at,
    CASE
        WHEN z.is_latest_run_zero = 1 THEN 'LATEST_RUN_EMPTY'
        ELSE 'HISTORICAL_ZEROS_ONLY'
    END AS alert_status
FROM zero_row_runs z
JOIN model_totals   m ON z.model_name = m.model_name
ORDER BY z.zero_row_run_count DESC, z.last_zero_row_at DESC
