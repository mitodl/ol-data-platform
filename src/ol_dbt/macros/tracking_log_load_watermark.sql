{% macro tracking_log_load_watermark(load_column='_dlt_load_id', stored_column='useractivity_load_id') %}
    {#-
        The incremental filter for a tracking-log user_activity model: raw rows from
        loads newer than the newest load already in {{ this }}.

        An event-time watermark (`time` > max(useractivity_timestamp)) skips every row
        whose file lands after a build has already passed its event time, which
        late-arriving logs do every day (0.07-0.47% of rows per source over 60 days,
        measured 2026-11-04). The load id orders rows by when they were loaded, so a
        late row is picked up by the first build after its load.

        dlt's _dlt_load_id is the load's unix timestamp as text with a fixed 10-digit
        integer part, so text order is load order. The stored maximum is inlined as a
        literal, so Iceberg can skip every raw file whose load id range is below it.

        The first build after the column is added has no stored load id. That build
        falls back to the event-time watermark, as before; the following builds use
        the load id.

        For a one-off backfill of rows an earlier event-time watermark skipped, pass
        `--vars '{tracking_log_reprocess_from: "YYYY-MM-DD"}'`. That reprocesses every
        raw row whose event date is on or after it. The model's unique_key and
        delete+insert replace the rows that are already there.
    -#}
    {%- set reprocess_from = var('tracking_log_reprocess_from', none) -%}
    {%- if reprocess_from is not none -%}
        substr({{ adapter.quote('time') }}, 1, 10) >= '{{ reprocess_from }}'
    {%- else -%}
        {%- set stored_load_id = none -%}
        {%- if stored_column in (adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list) -%}
            {%- set stored_load_id = run_query(
                'select max(' ~ stored_column ~ ') from ' ~ this
            ).columns[0].values()[0] -%}
        {%- endif -%}
        {%- if stored_load_id is none -%}
            {{ adapter.quote('time') }} > (select max(this.useractivity_timestamp) from {{ this }} as this)
        {%- else -%}
            {{ load_column }} > '{{ stored_load_id }}'
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
