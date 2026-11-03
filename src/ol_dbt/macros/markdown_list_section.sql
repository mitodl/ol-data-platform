{#
    markdown_list_section: a "## title" markdown section listing an array column's
    values, comma-separated or as bullets, or '' when the array is null or empty.
    The SQL literals carry real newlines, which Trino, DuckDB and StarRocks all accept.
#}
{% macro markdown_list_section(title, array_column, bullets=false) -%}
    case
        when coalesce({{ array_length(array_column) }}, 0) > 0
            then concat(
                '
## {{ title }}
',
                {% if bullets -%}
                    '- ', {{ array_join(array_column, '\n- ') }}
                {%- else -%}
                    {{ array_join(array_column, ', ') }}
                {%- endif %},
                '
'
            )
        else ''
    end
{%- endmacro %}
