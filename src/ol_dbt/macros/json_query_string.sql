{%- macro json_query_string(json_col, json_path) -%}
  {#
    Cross-database macro for extracting string values from JSON with the "omit quotes" behavior.

    This replaces the Trino-specific: json_query(col, 'lax $.path' omit quotes)

    Parameters:
      json_col: The column or expression containing JSON
      json_path: The JSON path WITHOUT 'lax' or 'omit quotes' keywords (e.g., '$.field' or '$.nested.field')

    Usage:
      {{ json_query_string('metadata_col', "'$.edx_video_id'") }}
      {{ json_query_string('event_object', "'$.problem_id'") }}
  #}
  {{- return(adapter.dispatch('json_query_string', 'open_learning')(json_col, json_path)) -}}
{%- endmacro -%}

{%- macro trino__json_query_string(json_col, json_path) -%}
  {# Trino: Use json_query with 'lax' mode and 'omit quotes' to return unquoted strings #}
  json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}' omit quotes)
{%- endmacro -%}

{%- macro duckdb__json_query_string(json_col, json_path) -%}
  {# DuckDB: Use json_extract_string which returns unquoted string values #}
  {# json_path is already a single-quoted SQL string literal (e.g. '$.field'), use it directly #}
  {# NULL for malformed JSON, as Trino's json_query (NULL ON ERROR) gives; json_extract_string raises #}
  case when json_valid({{ json_col }}) then json_extract_string({{ json_col }}, {{ json_path }}) end
{%- endmacro -%}

{%- macro starrocks__json_query_string(json_col, json_path) -%}
  {# StarRocks: Use get_json_string for direct string extraction #}
  {# Alternative: CAST(json_query(col, path) AS VARCHAR) #}
  get_json_string({{ json_col }}, {{ json_path }})
{%- endmacro -%}

{%- macro default__json_query_string(json_col, json_path) -%}
  {# Default to Trino behavior #}
  {{- return(trino__json_query_string(json_col, json_path)) -}}
{%- endmacro -%}

{%- macro json_query_object(json_col, key) -%}
  {#
    json_query_string for a top-level key whose value can be a JSON object or array, e.g. a
    problem event's answers or a studentmodule state's student_answers and correct_map.

    Trino's json_query(... omit quotes) returns an object as compact JSON with the source's
    key order. StarRocks' get_json_string sorts the keys and writes ", " and ": ", so the
    same event stored different text on each engine.

    Parameters:
      json_col: The column or expression containing JSON text
      key: A top-level key, bare (e.g. 'answers')
  #}
  {{- return(adapter.dispatch('json_query_object', 'open_learning')(json_col, key)) -}}
{%- endmacro -%}

{%- macro default__json_query_object(json_col, key) -%}
  {{- return(json_query_string(json_col, "'$." ~ key ~ "'")) -}}
{%- endmacro -%}

{%- macro starrocks__json_query_object(json_col, key) -%}
  {# The source text of the value, matched as an object with up to two levels of nesting.
     Quoted strings are consumed whole, so braces inside a string don't end the match. #}
  {%- set raw -%}
    regexp_extract({{ json_col }}, '"{{ key }}"\\s*:\\s*(\\{(?:"(?:[^"\\\\]|\\\\.)*"|[^"{}]|\\{(?:"(?:[^"\\\\]|\\\\.)*"|[^"{}])*\\})*\\})', 1)
  {%- endset -%}
  {%- set parsed = json_extract_json(json_col, "'$." ~ key ~ "'") -%}
  {# Use the compacted source text when it is the top-level value (the regexp can match the
     same key nested earlier) and has no \u or \/ escapes, which Trino decodes. Otherwise
     (those escapes, deeper nesting, or a top-level array) fall back to the parsed value:
     same content, but StarRocks sorts object keys, so it can differ from Trino in key
     order. #}
  case
    when {{ raw }} <> ''
      and not regexp({{ raw }}, '\\\\[u/]')
      and json_string(parse_json({{ raw }})) = json_string({{ parsed }})
      then {{ starrocks_compact_json_text(raw) }}
    when substr(json_string({{ parsed }}), 1, 1) in ('{', '[')
      then {{ json_to_varchar(parsed) }}
    else get_json_string({{ json_col }}, '$.{{ key }}')
  end
{%- endmacro -%}
