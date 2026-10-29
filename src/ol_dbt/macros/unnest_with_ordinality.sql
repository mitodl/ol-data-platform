{#
    unnest_with_ordinality: one row per element of `array_expr`, with the element's
    1-based position as a bigint, as Trino's UNNEST ... WITH ORDINALITY gives. A NULL
    or empty array yields no rows, and a NULL element yields a row with a NULL element.

    StarRocks has no WITH ORDINALITY, so it unnests the array beside a generated
    1..n array; a multi-array unnest pairs elements by position. array_generate(1, 0, 1)
    is empty and array_generate(1, NULL, 1) is NULL, so both yield no rows there too.
    `array_expr` appears twice in the StarRocks SQL, so pass a column rather than an
    expression that parses JSON.

    Usage (in FROM / CROSS JOIN clause):
      cross join {{ unnest_with_ordinality('programs.course_ids', 't', 'course_id', 'course_order') }}

    Parameters:
      array_expr:  expression yielding an array
      alias:       table alias for the result
      element_col: column name for each array element
      ordinal_col: column name for the element's 1-based position
#}
{% macro unnest_with_ordinality(array_expr, alias, element_col, ordinal_col) -%}
    {{ adapter.dispatch('unnest_with_ordinality', 'open_learning')(array_expr, alias, element_col, ordinal_col) }}
{%- endmacro %}

{% macro default__unnest_with_ordinality(array_expr, alias, element_col, ordinal_col) -%}
    unnest({{ array_expr }}) with ordinality as {{ alias }} ({{ element_col }}, {{ ordinal_col }})
{%- endmacro %}

{% macro starrocks__unnest_with_ordinality(array_expr, alias, element_col, ordinal_col) -%}
    {# The bigint start makes the ordinal bigint, the type Trino's ordinality column has. #}
    unnest(
        {{ array_expr }}
        , array_generate(cast(1 as bigint), array_length({{ array_expr }}), 1)
    ) as {{ alias }} ({{ element_col }}, {{ ordinal_col }})
{%- endmacro %}
