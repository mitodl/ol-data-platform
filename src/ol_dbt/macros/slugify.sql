{# Lowercase a label and collapse every run of non-alphanumerics to a single underscore,
   with no leading or trailing underscore: 'Payment / Refund!!' -> 'payment_refund'.
 #}
{% macro slugify(column) -%}
lower(regexp_replace(regexp_replace({{ column }}, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', ''))
{%- endmacro %}
