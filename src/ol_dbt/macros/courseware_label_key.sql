{#
    courseware_label_key: the normalized key concept labels are grouped and resolved
    by. Lowercase, then:
      1. a + or # at the start or after whitespace is a separator ("+ 5", "# of rows");
      2. any other + is " plus " and # is " sharp ", so C, C++ and C# stay apart;
      3. a dot at the start or after whitespace is "dot " (.NET, .csv);
      4. every run of characters that are not Unicode letters or numbers is one space,
         so R² keeps its 2 and Schrödinger its ö;
      5. leading and trailing whitespace is stripped, and a label with no letters or
         numbers left has no key (null).
#}
{% macro courseware_label_key(label_expr) -%}
    {%- set key = "lower(" ~ label_expr ~ ")" -%}
    {%- set key = regexp_replace_all(key, "'^[+#]'", "' '") -%}
    {%- set key = regexp_replace_all(key, "'\\s[+#]'", "' '") -%}
    {%- set key = regexp_replace_all(key, "'[+]'", "' plus '") -%}
    {%- set key = regexp_replace_all(key, "'#'", "' sharp '") -%}
    {%- set key = regexp_replace_all(key, "'^[.]'", "'dot '") -%}
    {%- set key = regexp_replace_all(key, "'\\s[.]'", "' dot '") -%}
    {%- set key = regexp_replace_all(key, "'[^\\p{L}\\p{N}]+'", "' '") -%}
    nullif({{ strip_whitespace(key) }}, '')
{%- endmacro %}
