{#-
    Compare what each raw table's last load holds with what its raw history
    snapshot holds open, for the lakehouse's history_load_not_short checks
    (dg_projects/lakehouse/lakehouse/assets/history_load_guard.py). Run as
    `dbt run-operation raw_history_load_guard --args '{tables: [...]}'`, where
    each entry names `snapshot`, `raw_table` and `unique_key` (a list).

    The load is the raw table's distinct keys, or the newest load's for a dlt
    merge feed raw_delete_filter reads by latest_load. A keyset table is
    read unfiltered: raw keeps a deleted key's row, so a key missing there
    means the load lost it, not that the source deleted it. The baseline
    is the snapshot's open, undeleted records, which is the previous load as
    the history recorded it. A snapshot that has not run yet has no baseline
    and passes.

    The open records whose key the load lacks trip with the rule and vars
    raw_history_delete_guard uses (raw_history_guard_trips). A per-table
    `max_delete_fraction` on the snapshot is not read here; no snapshot sets
    one and the generator writes none, so add it to `tables` before one does.
    Each table logs one line, `raw_history_load_guard {json}`, which the
    check parses.
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
        {%- set load_keys_sql -%}
            select distinct {{ keys | join(', ') }}
            from {{ raw }}
            {%- if newest_load %}
            where {{ newest_load }}
            {%- endif %}
        {%- endset -%}
        {%- set existing = adapter.get_relation(
            database=snapshot.database, schema=snapshot.schema, identifier=snapshot.identifier
        ) -%}
        {%- if existing is none -%}
            {%- set loaded = (run_query(
                "select count(*) from (" ~ load_keys_sql ~ ") as load_keys"
            ).rows[0][0] or 0) | int -%}
            {%- set current = none -%}
            {%- set missing = 0 -%}
        {%- else -%}
            {#- The same left join raw_history_delete_guard counts with, so a
                load that drops keys and adds as many new ones is still short. -#}
            {%- set guard_sql -%}
                with load_keys as (
                    {{ load_keys_sql }}
                )

                , current_keys as (
                    select distinct {{ keys | join(', ') }}
                    from {{ existing }}
                    where dbt_valid_to is null and coalesce(dbt_is_deleted, 'False') != 'True'
                )

                select
                    loaded.loaded_count
                    , compared.current_count
                    , compared.missing_count
                from (select count(*) as loaded_count from load_keys) as loaded
                cross join (
                    select
                        count(*) as current_count
                        , sum(case when load_keys.{{ keys[0] }} is null then 1 else 0 end)
                            as missing_count
                    from current_keys
                    left join load_keys
                        on
                        {%- for key in keys %}
                            {% if not loop.first %}and {% endif -%}
                            current_keys.{{ key }} = load_keys.{{ key }}
                        {%- endfor %}
                ) as compared
            {%- endset -%}
            {%- set row = run_query(guard_sql).rows[0] -%}
            {%- set loaded = (row[0] or 0) | int -%}
            {%- set current = (row[1] or 0) | int -%}
            {%- set missing = (row[2] or 0) | int -%}
        {%- endif -%}
        {%- set short = current is not none and raw_history_guard_trips(missing, current, limit, floor) -%}
        {{ log("raw_history_load_guard " ~ tojson({
            "snapshot": table['snapshot'],
            "raw_table": table['raw_table'],
            "loaded_keys": loaded,
            "snapshot_open_records": current,
            "missing_keys": missing,
            "max_fraction": limit,
            "min_count": floor,
            "short": short,
        }), info=true) }}
    {%- endfor -%}
{% endmacro %}
