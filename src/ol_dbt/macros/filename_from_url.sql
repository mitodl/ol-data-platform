{% macro filename_from_url(url) -%}
    {# Extract final path segment, excluding query parameters and fragments. #}
    {% set path = "split_part(split_part(" ~ url ~ ", '?', 1), '#', 1)" %}
    nullif({{ element_at_array("split(" ~ path ~ ", '/')", -1) }}, '')
{%- endmacro %}
