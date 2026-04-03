{#
    Cross-database compatibility macros for Trino, DuckDB, and StarRocks.

    These macros provide a unified interface for common SQL functions that have
    different implementations across database engines.
#}

{#
    json_extract_value: Cross-db extraction of a JSON value (returns JSON type, not plain string).
    This replaces the Trino-specific: json_query(col, 'lax $.path')

    For extracting plain strings use json_query_string instead.

    Parameters:
      json_col: The column or expression containing JSON
      json_path: The JSON path (e.g., "'$.metadata'", "'$.value.name'")

    Usage:
      {{ json_extract_value('block_details', "'$.metadata'") }}
#}
{% macro json_extract_value(json_col, json_path) -%}
    {{ adapter.dispatch('json_extract_value', 'open_learning')(json_col, json_path) }}
{%- endmacro %}

{% macro default__json_extract_value(json_col, json_path) -%}
    {# Trino: json_query with lax mode returns a JSON value #}
    json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}')
{%- endmacro %}

{% macro duckdb__json_extract_value(json_col, json_path) -%}
    {# DuckDB: json_extract returns a JSON value (equivalent to Trino json_query without omit quotes) #}
    json_extract({{ json_col }}, {{ json_path }})
{%- endmacro %}

{% macro starrocks__json_extract_value(json_col, json_path) -%}
    {# StarRocks: json_extract returns a JSON value #}
    json_extract({{ json_col }}, {{ json_path }})
{%- endmacro %}


{% macro from_iso8601_timestamp(timestamp_str) -%}
    {{ adapter.dispatch('from_iso8601_timestamp', 'open_learning')(timestamp_str) }}
{%- endmacro %}

{% macro default__from_iso8601_timestamp(timestamp_str) -%}
    {# Trino: native support #}
    from_iso8601_timestamp({{ timestamp_str }})
{%- endmacro %}

{% macro duckdb__from_iso8601_timestamp(timestamp_str) -%}
    {# DuckDB: use strptime or cast #}
    cast({{ timestamp_str }} as timestamp)
{%- endmacro %}

{% macro starrocks__from_iso8601_timestamp(timestamp_str) -%}
    {# StarRocks: use str_to_date or cast #}
    cast({{ timestamp_str }} as datetime)
{%- endmacro %}


{% macro from_iso8601_timestamp_nanos(timestamp_str) -%}
    {{ adapter.dispatch('from_iso8601_timestamp_nanos', 'open_learning')(timestamp_str) }}
{%- endmacro %}

{% macro default__from_iso8601_timestamp_nanos(timestamp_str) -%}
    {# Trino: native nanosecond-precision timestamp parser #}
    from_iso8601_timestamp_nanos({{ timestamp_str }})
{%- endmacro %}

{% macro duckdb__from_iso8601_timestamp_nanos(timestamp_str) -%}
    {# DuckDB: max precision is microseconds; cast ISO 8601 string as timestamptz #}
    try_cast({{ timestamp_str }} as timestamptz)
{%- endmacro %}


{% macro array_join(array_expr, delimiter, null_replacement='') -%}
    {{ adapter.dispatch('array_join', 'open_learning')(array_expr, delimiter, null_replacement) }}
{%- endmacro %}

{% macro default__array_join(array_expr, delimiter, null_replacement='') -%}
    {# Trino: native support #}
    {% if null_replacement %}
        array_join({{ array_expr }}, '{{ delimiter }}', '{{ null_replacement }}')
    {% else %}
        array_join({{ array_expr }}, '{{ delimiter }}')
    {% endif %}
{%- endmacro %}

{% macro duckdb__array_join(array_expr, delimiter, null_replacement='') -%}
    {# DuckDB: use array_to_string or list_aggr #}
    array_to_string({{ array_expr }}, '{{ delimiter }}')
{%- endmacro %}

{% macro starrocks__array_join(array_expr, delimiter, null_replacement='') -%}
    {# StarRocks: array_join with different signature #}
    array_join({{ array_expr }}, '{{ delimiter }}')
{%- endmacro %}


{% macro regexp_like(string_expr, pattern) -%}
    {{ adapter.dispatch('regexp_like', 'open_learning')(string_expr, pattern) }}
{%- endmacro %}

{% macro default__regexp_like(string_expr, pattern) -%}
    {# Trino: native support #}
    regexp_like({{ string_expr }}, {{ pattern }})
{%- endmacro %}

{% macro duckdb__regexp_like(string_expr, pattern) -%}
    {# DuckDB: use regexp_matches #}
    regexp_matches({{ string_expr }}, {{ pattern }})
{%- endmacro %}

{% macro starrocks__regexp_like(string_expr, pattern) -%}
    {# StarRocks: regexp #}
    {{ string_expr }} regexp {{ pattern }}
{%- endmacro %}


{% macro element_at_array(array_expr, index) -%}
    {{ adapter.dispatch('element_at_array', 'open_learning')(array_expr, index) }}
{%- endmacro %}

{% macro default__element_at_array(array_expr, index) -%}
    {# Trino: element_at with 1-based indexing #}
    element_at({{ array_expr }}, {{ index }})
{%- endmacro %}

{% macro duckdb__element_at_array(array_expr, index) -%}
    {# DuckDB: array subscript with 1-based indexing (list_element also works) #}
    ({{ array_expr }})[{{ index }}]
{%- endmacro %}

{% macro starrocks__element_at_array(array_expr, index) -%}
    {# StarRocks: array subscript with 1-based indexing #}
    {{ array_expr }}[{{ index }}]
{%- endmacro %}


{#
    format_datetime: Format a date/timestamp using a Java-style (Trino) or strftime-style (DuckDB) pattern.
    For cross-db use, map from Java DateTime format (Trino) to strftime format (DuckDB).
    Common mappings: 'yyyyMMdd' -> '%Y%m%d', 'EEEE' -> '%A', 'MMMM' -> '%B'
#}
{% macro format_datetime(datetime_expr, java_format) -%}
    {{ adapter.dispatch('format_datetime', 'open_learning')(datetime_expr, java_format) }}
{%- endmacro %}

{% macro default__format_datetime(datetime_expr, java_format) -%}
    {# Trino: native format_datetime with Java DateTime format #}
    format_datetime({{ datetime_expr }}, '{{ java_format }}')
{%- endmacro %}

{% macro duckdb__format_datetime(datetime_expr, java_format) -%}
    {# DuckDB: strftime with %-style format. Convert common Java patterns. #}
    {% set strftime_format = java_format
        | replace('yyyy', '%Y')
        | replace('MMMM', '%B')
        | replace('MMM', '%b')
        | replace('MM', '%m')
        | replace('dd', '%d')
        | replace('EEEE', '%A')
        | replace('EEE', '%a')
        | replace('HH', '%H')
        | replace('mm', '%M')
        | replace('ss', '%S')
    %}
    strftime({{ datetime_expr }}, '{{ strftime_format }}')
{%- endmacro %}


{#
    date_format: Format a date/timestamp using a MySQL-style format string (Trino date_format).
    DuckDB uses strftime with the same %-style format strings.
#}
{% macro date_format(datetime_expr, format_string) -%}
    {{ adapter.dispatch('date_format', 'open_learning')(datetime_expr, format_string) }}
{%- endmacro %}

{% macro default__date_format(datetime_expr, format_string) -%}
    {# Trino: date_format with MySQL-style format string #}
    date_format({{ datetime_expr }}, {{ format_string }})
{%- endmacro %}

{% macro duckdb__date_format(datetime_expr, format_string) -%}
    {# DuckDB: strftime uses same %-style format strings as Trino date_format #}
    strftime({{ datetime_expr }}, {{ format_string }})
{%- endmacro %}


{#
    day_of_week: ISO day of week (1=Monday, 7=Sunday) consistent with Trino day_of_week().
#}
{% macro day_of_week(date_expr) -%}
    {{ adapter.dispatch('day_of_week', 'open_learning')(date_expr) }}
{%- endmacro %}

{% macro default__day_of_week(date_expr) -%}
    {# Trino: day_of_week returns 1=Mon, 7=Sun #}
    day_of_week({{ date_expr }})
{%- endmacro %}

{% macro duckdb__day_of_week(date_expr) -%}
    {# DuckDB: isodow returns 1=Mon, 7=Sun (same as Trino) #}
    isodow({{ date_expr }})
{%- endmacro %}


{#
    iso8601_to_date_key: Convert an ISO8601 varchar date/datetime field to an integer YYYYMMDD date key.
    Handles both 'YYYY-MM-DD' (10 chars) and 'YYYY-MM-DDTHH:MM:SS...' (>=19 chars) formats.
    Returns NULL if input is NULL.
#}
{% macro iso8601_to_date_key(varchar_field) -%}
    {{ return(adapter.dispatch('iso8601_to_date_key', 'open_learning')(varchar_field)) }}
{%- endmacro %}

{% macro default__iso8601_to_date_key(varchar_field) -%}
    {# Trino: date_parse + date_format #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(date_format(date_parse({{ varchar_field }}, '%Y-%m-%d'), '%Y%m%d') AS INTEGER)
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(date_format(date_parse(SUBSTR({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%i:%s'), '%Y%m%d') AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}

{% macro duckdb__iso8601_to_date_key(varchar_field) -%}
    {# DuckDB: try_strptime returns NULL on invalid input instead of throwing #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(strftime(try_strptime({{ varchar_field }}, '%Y-%m-%d'), '%Y%m%d') AS INTEGER)
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(strftime(try_strptime(SUBSTR({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%M:%S'), '%Y%m%d') AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}


{#
    last_value_ignore_nulls: Cross-db wrapper for last_value with IGNORE NULLS.
    Trino: last_value(expr) IGNORE NULLS OVER (window)
    DuckDB: last_value(expr IGNORE NULLS) OVER (window)

    Usage (write the OVER clause inline after the macro call):
      {{ last_value_ignore_nulls('my_expr') }} over (partition by ... order by ...)
#}
{% macro last_value_ignore_nulls(expr) -%}
    {{ adapter.dispatch('last_value_ignore_nulls', 'open_learning')(expr) }}
{%- endmacro %}

{% macro default__last_value_ignore_nulls(expr) -%}
    {# Trino: IGNORE NULLS sits after the closing paren, before OVER #}
    last_value({{ expr }}) ignore nulls
{%- endmacro %}

{% macro duckdb__last_value_ignore_nulls(expr) -%}
    {# DuckDB: IGNORE NULLS sits inside the function arguments #}
    last_value({{ expr }} ignore nulls)
{%- endmacro %}



{#
    unnest_json_map: Cross-db unnesting of a JSON object into (key, value) rows.
    Trino: UNNEST(cast(expr as map(varchar, json))) AS alias(key_col, val_col)
    DuckDB: subquery using map_keys() / map_values() as parallel array unnests

    Usage (in FROM / CROSS JOIN clause):
      cross join {{ unnest_json_map('json_expr', 't', 'key', 'value') }}

    Parameters:
      json_expr: expression yielding a JSON object to iterate
      alias:     table alias for the result
      key_col:   column name for the map key
      val_col:   column name for the map value
#}
{% macro unnest_json_map(json_expr, alias, key_col, val_col) -%}
    {{ adapter.dispatch('unnest_json_map', 'open_learning')(json_expr, alias, key_col, val_col) }}
{%- endmacro %}

{% macro default__unnest_json_map(json_expr, alias, key_col, val_col) -%}
    {#
        Trino: parse JSON object into map(varchar, json), then serialize each value back to varchar
        using json_format() (cast(json as varchar) is not supported in Trino; use json_format instead).
        Downstream json_query_string calls then receive valid JSON text as varchar.
        try_cast returns NULL for non-JSON input → transform_values(NULL, ...) = NULL → UNNEST = 0 rows.
    #}
    unnest(
        transform_values(
            try_cast({{ json_expr }} as map(varchar, json)),
            (k, v) -> json_format(v)
        )
    ) as {{ alias }}({{ key_col }}, {{ val_col }})
{%- endmacro %}

{% macro duckdb__unnest_json_map(json_expr, alias, key_col, val_col) -%}
    {#
        DuckDB does not support UNNEST on a MAP type directly in a CROSS JOIN.
        Use parallel unnests of map_keys() and map_values() in a subquery.
        Callers are responsible for pre-filtering non-object JSON values (e.g.
        with json_is_object()) before data reaches this cross join, since
        DuckDB may reorder WHERE clauses past the cast in its execution plan.
        DuckDB aligns multiple UNNESTs in the same SELECT positionally.
    #}
    (
        select
            unnest(map_keys(cast({{ json_expr }} as map(varchar, json)))) as {{ key_col }}
            , unnest(map_values(cast({{ json_expr }} as map(varchar, json)))) as {{ val_col }}
    ) as {{ alias }}
{%- endmacro %}


{#
    json_is_object: Cross-db predicate that returns TRUE when a JSON expression
    is a JSON object (as opposed to a string, array, number, etc.).
    Use in WHERE clauses to pre-filter non-object values before passing to
    unnest_json_map(), which requires a MAP-castable (object) JSON input.

    Parameters:
      json_expr: expression yielding a JSON value to test

    Usage:
      WHERE {{ json_is_object("json_extract(col, '$.field')") }}
#}
{% macro json_is_object(json_expr) -%}
    {{ adapter.dispatch('json_is_object', 'open_learning')(json_expr) }}
{%- endmacro %}

{% macro default__json_is_object(json_expr) -%}
    {# Trino: json_extract returns native json type; use json_format() to serialize to varchar.
       cast(json as varchar) is NOT supported in Trino; json_format() is the correct function. #}
    substr(json_format({{ json_expr }}), 1, 1) = '{'
{%- endmacro %}

{% macro duckdb__json_is_object(json_expr) -%}
    {# DuckDB: json_type() returns 'OBJECT' for JSON objects #}
    json_type({{ json_expr }}) = 'OBJECT'
{%- endmacro %}


{% macro is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
    {{ adapter.dispatch('is_courserun_current', 'open_learning')(start_on_timestamp_str, end_on_timestamp_str) }}
{%- endmacro %}

{% macro default__is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
   {# Trino: native support #}
    case
        when
            cast(from_iso8601_timestamp({{ start_on_timestamp_str }}) as date) <= current_date
            and (
                {{ end_on_timestamp_str }} is null
                or cast(from_iso8601_timestamp({{ end_on_timestamp_str }}) as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}

{% macro duckdb__is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
    case
        when
            cast({{ start_on_timestamp_str }} as date) <= current_date
            and (
                {{ end_on_timestamp_str }} is null
                or cast({{ end_on_timestamp_str }} as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}
