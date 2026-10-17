{#
    string_position: the 1-based position of substring_text in string_text, 0 when it
    is absent. dbt.position renders the SQL-standard position(x in y) on every adapter,
    and dbt-starrocks does not override it; StarRocks gets MySQL's locate(substr, str).
#}
{% macro string_position(substring_text, string_text) -%}
    {{ adapter.dispatch('string_position', 'open_learning')(substring_text, string_text) }}
{%- endmacro %}

{% macro default__string_position(substring_text, string_text) -%}
    strpos({{ string_text }}, {{ substring_text }})
{%- endmacro %}

{% macro starrocks__string_position(substring_text, string_text) -%}
    locate({{ substring_text }}, {{ string_text }})
{%- endmacro %}
