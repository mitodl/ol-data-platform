{#
    courseware_label_key: the normalized key concept labels are grouped and resolved
    by. A label with no Unicode letter or number has no key (null), so ".", ".+" and
    "#" do not group with a concept named "dot". Otherwise lowercase, then:
      1. a + or # at the start or after whitespace is a separator ("+ 5", "# of rows");
      2. any other + is " plus " and # is " sharp ", so C, C++ and C# stay apart;
      3. a dot at the start or after whitespace is "dot " (.NET, .csv);
      4. every run of characters that are not Unicode letters or numbers is one space,
         so R² keeps its 2 and Schrödinger its ö;
      5. leading and trailing whitespace is stripped.
    ml.lib.courseware.tagging.label_key is the same rule in Python and must match it.
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
    case
        when {{ regexp_like(label_expr, "'[\\p{L}\\p{N}]'") }}
            then {{ strip_whitespace(key) }}
    end
{%- endmacro %}
