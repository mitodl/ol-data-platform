{#-
    Compare what each raw table's last load holds with what its raw history
    snapshot holds open, for the lakehouse's history_load_not_short checks
    (dg_projects/lakehouse/lakehouse/assets/history_load_guard.py). Run as
    `dbt run-operation raw_history_load_guard --args '{tables: [...]}'`, where
    each entry names `snapshot`, `raw_table` and `unique_key` (a list).

    The load is the raw table's distinct keys, or the newest load's for a dlt
    merge feed raw_delete_filter reads by latest_load. A keyset table is
    counted unfiltered: raw keeps a deleted key's row, so a short count there
    means the load lost rows, not that the source deleted them. The baseline
    is the snapshot's open, undeleted records, which is the previous load as
    the history recorded it. A snapshot that has not run yet has no baseline
    and passes.

    The shortfall trips with the rule and vars raw_history_delete_guard uses
    (raw_history_guard_trips), so the load-side and snapshot-side guards fail
    on the same numbers. Each table logs one line,
    `raw_history_load_guard {json}`, which the check parses.
-#}
{% macro raw_history_load_guard(tables) %}
    {%- set limit, floor = raw_history_guard_limits() -%}
    {%- for table in tables -%}
        {%- set keys = [] -%}
        {%- for key in table['unique_key'] -%}
            {%- do keys.append(key if modules.re.fullmatch('[a-z_][a-z0-9_]*', key) else adapter.quote(key)) -%}
        {%- endfor -%}
        {%- set raw = source('ol_warehouse_raw_data', table['raw_table']) -%}
        {%- set snapshot = ref(table['snapshot']) -%}
        {%- set spec = raw_delete_filter_map().get(table['raw_table']) -%}
        {%- set newest_load = raw_delete_filter(table['raw_table'], raw)
            if spec is not none and spec['kind'] == 'latest_load' else none -%}
        {%- set load_sql -%}
            select count(*) from (
                select distinct {{ keys | join(', ') }}
                from {{ raw }}
                {%- if newest_load %}
                where {{ newest_load }}
                {%- endif %}
            ) as load_keys
        {%- endset -%}
        {%- set loaded = (run_query(load_sql).rows[0][0] or 0) | int -%}
        {%- set existing = adapter.get_relation(
            database=snapshot.database, schema=snapshot.schema, identifier=snapshot.identifier
        ) -%}
        {%- if existing is none -%}
            {%- set current = none -%}
        {%- else -%}
            {%- set current_sql -%}
                select count(*) from {{ existing }}
                where dbt_valid_to is null and coalesce(dbt_is_deleted, 'False') != 'True'
            {%- endset -%}
            {%- set current = (run_query(current_sql).rows[0][0] or 0) | int -%}
        {%- endif -%}
        {%- set shortfall = [current - loaded, 0] | max if current is not none else 0 -%}
        {%- set short = current is not none and raw_history_guard_trips(shortfall, current, limit, floor) -%}
        {{ log("raw_history_load_guard " ~ tojson({
            "snapshot": table['snapshot'],
            "raw_table": table['raw_table'],
            "loaded_keys": loaded,
            "snapshot_open_records": current,
            "shortfall": shortfall,
            "max_fraction": limit,
            "min_count": floor,
            "short": short,
        }), info=true) }}
    {%- endfor -%}
{% endmacro %}
