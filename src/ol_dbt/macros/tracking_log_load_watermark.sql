{% macro tracking_log_load_watermark(
    load_column='_dlt_load_id', stored_column='useractivity_load_id', lookback_seconds=10800
) %}
    {#-
        The incremental filter for a tracking-log user_activity model: raw rows from
        loads at or after the newest load already in {{ this }}, less a lookback.

        An event-time watermark (`time` > max(useractivity_timestamp)) skips every row
        whose file lands after a build has already passed its event time, which
        late-arriving logs do every day (0.07-0.47% of rows per source over 60 days,
        measured 2026-11-04). The load id orders rows by when they were loaded, so a
        late row is picked up by the first build after its load.

        dlt's _dlt_load_id is the load's unix timestamp as text with a fixed 10-digit
        integer part, so text order is load order. The bound is inlined as a literal,
        so Iceberg can skip every raw file whose load id range is below it.

        The bound is the stored maximum less `lookback_seconds` (3 hours), compared
        with >=. A load that commits after a newer one (an overlapping manual run), or
        one load written in several commits with a build in between, would otherwise
        be skipped. The rows re-read from the lookback replace themselves through the
        model's unique_key and delete+insert.

        The first build after the column is added has no stored load id. That build
        falls back to the event-time watermark, as before; the following builds use
        the load id.

        For a one-off backfill of rows an earlier event-time watermark skipped, pass
        `--vars '{tracking_log_reprocess_from: "YYYY-MM-DD"}'`. That reprocesses every
        raw row whose event date (the raw text's first 10 characters, so start a day
        early for events logged with a non-UTC offset) is on or after it, through
        today. That scans the whole raw table. The model's unique_key and
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
            {{ load_column }} >= '{{ "%.6f" | format(stored_load_id | float - lookback_seconds) }}'
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
