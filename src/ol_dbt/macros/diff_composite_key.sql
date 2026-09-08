{% macro diff_composite_key(field_list) %}
  {{ return(adapter.dispatch('diff_composite_key', 'open_learning')(field_list)) }}
{% endmacro %}

{% macro default__diff_composite_key(field_list) %}
{#-
  Collapse a composite key into ONE unambiguous scalar join column, for
  `ol-dbt diff`'s per-column comparison. audit_helper.compare_column_values
  emits `a_query.{{ primary_key }} = b_query.{{ primary_key }}`, so it can only
  join on a single column; this is what that column is built from.

  Why not dbt_utils.generate_surrogate_key: it joins components with a literal
  '-' before hashing, which does not encode component boundaries. It therefore
  maps ('a-b', 'c') and ('a', 'b-c') to the SAME hash -- verified on dbt_utils
  1.3.3 -- so two distinct keys would pair as one row and reintroduce exactly
  the many-to-many mispairing the composite key is there to prevent.

  Here each component is length-prefixed as `<len>:<value>`. Reading digits up
  to the first ':' gives the length, and the next <len> characters are the value,
  so no character inside a value can shift a boundary. The encoding is injective
  regardless of whether the adapter's length() counts bytes or characters, since
  both sides of any one comparison are computed by the same engine.

  NULL renders as '~'. A non-null component always renders as '<digits>:<value>',
  so a null can never collide with a real value. Nulls still PAIR (rather than
  being dropped by a plain equi-join), which is what makes a diff on a nullable
  key useful.

  The '|' between components is for legibility only -- the length prefixes
  already make the encoding unambiguous without it.
-#}
{%- set parts = [] -%}
{%- for field in field_list -%}
  {%- set value = "cast(" ~ field ~ " as " ~ dbt.type_string() ~ ")" -%}
  {%- set encoded = dbt.concat([
       "cast(" ~ dbt.length(value) ~ " as " ~ dbt.type_string() ~ ")",
       "':'",
       value,
     ]) -%}
  {%- do parts.append("case when " ~ field ~ " is null then '~' else " ~ encoded ~ " end") -%}
  {%- if not loop.last -%}
    {%- do parts.append("'|'") -%}
  {%- endif -%}
{%- endfor -%}
{{ dbt.hash(dbt.concat(parts)) }}
{% endmacro %}
