{% macro dlt_load_id_floor(stored_load_id, lookback_seconds=10800) -%}
    {#-
        The lower bound, as a dlt load id literal, for re-reading rows loaded at or after
        `stored_load_id` less `lookback_seconds` (3 hours).

        dlt's _dlt_load_id is the load's unix timestamp as text with a fixed 10-digit
        integer part, so text order is load order and the bound compares as text. The
        lookback covers a load that commits after a newer one, or one load written in
        several commits with a build in between.
    -#}
    {{- "%.6f" | format(stored_load_id | float - lookback_seconds) -}}
{%- endmacro %}
