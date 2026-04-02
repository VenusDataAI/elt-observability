-- =============================================================================
-- observability_hooks.sql
-- dbt macros for logging model execution metadata to a warehouse table.
--
-- Usage in dbt_project.yml:
--   models:
--     your_project:
--       +post-hook: "{{ observability_post_hook() }}"
--
-- Or on a single model:
--   {{ config(post_hook="{{ observability_post_hook() }}") }}
-- =============================================================================

{% macro log_observability(model_name, status, rows) %}
  {#-
    Logs an observability event to the _observability_log table.
    Creates the table if it does not exist.
  -#}

  {%- set sql -%}
    CREATE TABLE IF NOT EXISTS {{ target.schema }}._observability_log (
        logged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        invocation_id   VARCHAR(64),
        model_name      VARCHAR(256),
        run_started_at  VARCHAR(64),
        status          VARCHAR(32),
        rows_affected   BIGINT
    );

    INSERT INTO {{ target.schema }}._observability_log
        (invocation_id, model_name, run_started_at, status, rows_affected)
    VALUES (
        '{{ invocation_id }}',
        '{{ model_name }}',
        '{{ run_started_at }}',
        '{{ status }}',
        {{ rows | default(0) }}
    );
  {%- endset -%}

  {{ log("[OBSERVABILITY] model=" ~ model_name ~ " status=" ~ status ~ " rows=" ~ rows, info=True) }}

  {% if execute %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}


{% macro observability_post_hook() %}
  {#-
    Intended to be called as a post-hook. Captures rowcount from the adapter.
    Compatible with BigQuery, Snowflake, Redshift, DuckDB, and Postgres adapters.
  -#}
  {{ log_observability(
      model_name=this.name,
      status='success',
      rows=adapter.get_rows_affected() if adapter.get_rows_affected is defined else -1
  ) }}
{% endmacro %}


{% macro observability_test_hook(model_name, rows=0) %}
  {#- Thin wrapper for testing the macro without a live adapter -#}
  {{ log_observability(model_name=model_name, status='test', rows=rows) }}
{% endmacro %}
