{# Lowercase a label and collapse every run of non-alphanumerics to a single underscore,
   with no leading or trailing underscore: 'Payment / Refund!!' -> 'payment_refund'.
 #}
{% macro slugify(column) -%}
lower(regexp_replace(regexp_replace({{ column }}, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', ''))
{%- endmacro %}


{# Django's slugify (django.utils.text), which MIT Learn builds ids with: 'January IAP' ->
   'january-iap', ' Été - ' -> 'ete', 'Café ½' -> 'cafe-12'. The string is NFKD-normalized
   and loses its non-ASCII characters first, so the whitespace set is Python's ASCII \s.
   See nfkd_normalize for where DuckDB and StarRocks fold differently.
 #}
{% macro django_slugify(column) -%}
    {%- set space = "\\x{9}-\\x{d}\\x{1c}-\\x{20}" -%}
    {%- set ascii = regexp_replace_all(nfkd_normalize(column), "'[^\\x{0}-\\x{7f}]'", "''") -%}
    {%- set kept = regexp_replace_all("lower(" ~ ascii ~ ")", "'[^a-z0-9_" ~ space ~ "-]'", "''") -%}
    {%- set dashed = regexp_replace_all(kept, "'[" ~ space ~ "-]+'", "'-'") -%}
    {{ regexp_replace_all(regexp_replace_all(dashed, "'^[-_]+'", "''"), "'[-_]+$'", "''") }}
{%- endmacro %}
