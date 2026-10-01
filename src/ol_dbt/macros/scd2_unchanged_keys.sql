{#
    Body of a CTE listing the natural keys an SCD2 merge has nothing to do for: the
    distinct tracked versions of the key's incoming rows are exactly the distinct tracked
    versions of its current rows in `existing`, and the key has as many current rows as
    incoming rows.

    Comparing the sets and the row counts is what makes the merge self-healing. If
    upstream briefly carries two conflicting copies of a key, both land as current rows.
    Once upstream recovers to one row, that row still matches one of them, so a per-row
    `not exists` check would skip the key and the stale copy would stay current forever.
    Here the mismatch marks the key changed, so the model expires all of its current rows
    and writes the incoming rows as the current versions.

    `incoming` must already carry the target's column names and be distinct.
#}
{% macro scd2_unchanged_keys(incoming, existing, key_columns, tracked_columns) %}
    {%- set version_columns = key_columns + tracked_columns -%}
    select
        {{ _scd2_column_list(key_columns, 'incoming_versions') }}
    from (
        select
            {{ _scd2_column_list(key_columns, 'incoming_version_rows') }}
            , count(*) as incoming_version_count
            , count(current_version_rows.{{ key_columns[0] }}) as matched_version_count
        from (
            select distinct {{ _scd2_column_list(version_columns) }}
            from {{ incoming }}
        ) as incoming_version_rows
        left join (
            select distinct {{ _scd2_column_list(version_columns) }}
            from {{ existing }}
            where is_current = true
        ) as current_version_rows
            on
            {% for key in key_columns -%}
                incoming_version_rows.{{ key }} = current_version_rows.{{ key }}
                and
            {% endfor -%}
            {% for column in tracked_columns -%}
                (
                    incoming_version_rows.{{ column }} = current_version_rows.{{ column }}
                    or (incoming_version_rows.{{ column }} is null and current_version_rows.{{ column }} is null)
                )
                {{ "and" if not loop.last }}
            {% endfor %}
        group by {{ _scd2_column_list(key_columns, 'incoming_version_rows') }}
    ) as incoming_versions
    inner join (
        select {{ _scd2_column_list(key_columns) }}, count(*) as current_version_count
        from (
            select distinct {{ _scd2_column_list(version_columns) }}
            from {{ existing }}
            where is_current = true
        ) as distinct_current_versions
        group by {{ _scd2_column_list(key_columns) }}
    ) as current_versions
        on {{ _scd2_key_join(key_columns, 'incoming_versions', 'current_versions') }}
    inner join (
        select {{ _scd2_column_list(key_columns) }}, count(*) as incoming_row_count
        from {{ incoming }}
        group by {{ _scd2_column_list(key_columns) }}
    ) as incoming_rows
        on {{ _scd2_key_join(key_columns, 'incoming_versions', 'incoming_rows') }}
    inner join (
        select {{ _scd2_column_list(key_columns) }}, count(*) as current_row_count
        from {{ existing }}
        where is_current = true
        group by {{ _scd2_column_list(key_columns) }}
    ) as current_rows
        on {{ _scd2_key_join(key_columns, 'incoming_versions', 'current_rows') }}
    where
        incoming_versions.matched_version_count = incoming_versions.incoming_version_count
        and current_versions.current_version_count = incoming_versions.incoming_version_count
        and current_rows.current_row_count = incoming_rows.incoming_row_count
{% endmacro %}

{% macro _scd2_column_list(columns, relation=none) -%}
    {%- for column in columns -%}
        {{ relation ~ "." if relation }}{{ column }}{{ ", " if not loop.last }}
    {%- endfor -%}
{%- endmacro %}

{% macro _scd2_key_join(key_columns, left, right) -%}
    {%- for key in key_columns -%}
        {{ left }}.{{ key }} = {{ right }}.{{ key }}{{ " and " if not loop.last }}
    {%- endfor -%}
{%- endmacro %}
