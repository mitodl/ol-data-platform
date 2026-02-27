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
  json_extract_string({{ json_col }}, {{ json_path | replace("'", "\"") }})
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
