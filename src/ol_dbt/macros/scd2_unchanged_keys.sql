{#
    Body of a CTE listing the natural keys an SCD2 merge has nothing to do for: every
    incoming row matches a current row in `existing` on `tracked_columns`, and the key has
    exactly as many current rows as incoming rows.

    The row-count check is what makes the merge self-healing. If upstream briefly carries
    two conflicting copies of a key, both land as current rows. Once upstream recovers to
    one row, that row still matches one of them, so a per-row `not exists` check would skip
    the key and the stale copy would stay current forever. Here the count mismatch marks
    the key changed, so the model expires all of its current rows and writes the incoming
    row as the only current version.

    `incoming` must already carry the target's column names and be distinct.
#}
{% macro scd2_unchanged_keys(incoming, existing, key_columns, tracked_columns) %}
    {%- set match_marker = key_columns[0] -%}
    select
        {% for key in key_columns -%}
            incoming_counts.{{ key }}{{ "," if not loop.last }}
        {% endfor %}
    from (
        select
            {% for key in key_columns -%}
                incoming_rows.{{ key }},
            {% endfor -%}
            count(*) as incoming_row_count
            , count(existing_versions.{{ match_marker }}) as matched_row_count
        from {{ incoming }} as incoming_rows
        left join (
            select distinct
                {% for column in key_columns + tracked_columns -%}
                    {{ column }}{{ "," if not loop.last }}
                {% endfor %}
            from {{ existing }}
            where is_current = true
        ) as existing_versions
            on
            {% for key in key_columns -%}
                incoming_rows.{{ key }} = existing_versions.{{ key }}
                and
            {% endfor -%}
            {% for column in tracked_columns -%}
                (
                    incoming_rows.{{ column }} = existing_versions.{{ column }}
                    or (incoming_rows.{{ column }} is null and existing_versions.{{ column }} is null)
                )
                {{ "and" if not loop.last }}
            {% endfor %}
        group by
            {% for key in key_columns -%}
                incoming_rows.{{ key }}{{ "," if not loop.last }}
            {% endfor %}
    ) as incoming_counts
    inner join (
        select
            {% for key in key_columns -%}
                {{ key }},
            {% endfor -%}
            count(*) as current_row_count
        from {{ existing }}
        where is_current = true
        group by
            {% for key in key_columns -%}
                {{ key }}{{ "," if not loop.last }}
            {% endfor %}
    ) as current_counts
        on
        {% for key in key_columns -%}
            incoming_counts.{{ key }} = current_counts.{{ key }}
            {{ "and" if not loop.last }}
        {% endfor %}
    where
        incoming_counts.matched_row_count = incoming_counts.incoming_row_count
        and current_counts.current_row_count = incoming_counts.incoming_row_count
{% endmacro %}
