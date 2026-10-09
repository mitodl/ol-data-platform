{#
    Cross-database compatibility macros for Trino, DuckDB, and StarRocks.

    These macros provide a unified interface for common SQL functions that have
    different implementations across database engines.
#}

{#
    json_extract_value: Cross-db extraction of a JSON value. On Trino this returns varchar
    (JSON-formatted text), NOT a native JSON value -- safe to select directly as a persisted
    model column. On DuckDB/StarRocks it returns a native JSON value (those connectors don't
    have this restriction).

    This replaces the Trino-specific: json_query(col, 'lax $.path')

    IMPORTANT (confirmed against a live Trino cluster): do not "fix" the Trino body to return a
    native JSON value. A live `json`-typed Trino value cannot be written to an Iceberg v2 table --
    it fails at write time with "Invalid schema for v2: ... variant is not supported until v3".
    Every current caller of this macro selects its result directly as an output column, so it
    must stay varchar on Trino. If you need a native JSON value for intra-query composition
    (e.g. feeding unnest_json_map/json_is_object) and never select the result as a persisted
    column, use json_extract_json instead.

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
    {# Trino: json_query with lax mode returns varchar (JSON-formatted text), not a native JSON
       value. See the macro docstring above -- do not change this to return native JSON. #}
    json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}')
{%- endmacro %}

{% macro duckdb__json_extract_value(json_col, json_path) -%}
    {# DuckDB: json_extract returns a JSON value (equivalent to Trino json_query without omit quotes) #}
    json_extract({{ json_col }}, {{ json_path }})
{%- endmacro %}

{% macro starrocks__json_extract_value(json_col, json_path) -%}
    {# StarRocks: json_extract does not exist; use json_query on a parsed JSON value #}
    json_query(parse_json({{ json_col }}), {{ json_path }})
{%- endmacro %}


{#
    json_extract_json: Cross-db extraction of a JSON value as a native JSON-typed expression, for
    INTRA-QUERY composition only (e.g. feeding unnest_json_map/json_is_object). Do NOT select this
    directly as a persisted model column -- on Trino a live JSON value cannot be written to an
    Iceberg v2 table ("Invalid schema for v2: ... variant is not supported until v3"). For a
    column you select as output, use json_extract_value instead.

    Parameters:
      json_col: The column or expression containing JSON
      json_path: The JSON path (e.g., "'$.metadata'", "'$.value.name'")

    Usage:
      {{ unnest_json_map(json_extract_json('block_metadata', "'$.discussion_topics'"), 't', 'key', 'value') }}
#}
{% macro json_extract_json(json_col, json_path) -%}
    {{ adapter.dispatch('json_extract_json', 'open_learning')(json_col, json_path) }}
{%- endmacro %}

{% macro default__json_extract_json(json_col, json_path) -%}
    {# Trino: json_parse the varchar json_query result back into a native JSON value. `RETURNING
       json` is not a valid way to get there directly -- confirmed against a live Trino cluster,
       it errors with "Cannot output JSON value as json using formatting JSON" once the result
       flows into callers like unnest_json_map's map(varchar, json) cast or json_is_object's
       json_format(). This json_parse(json_query(...)) pattern is the same one already used by
       json_extract_varchar_array. #}
    json_parse(json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}'))
{%- endmacro %}

{% macro duckdb__json_extract_json(json_col, json_path) -%}
    {# DuckDB: json_extract already returns a native JSON value #}
    json_extract({{ json_col }}, {{ json_path }})
{%- endmacro %}

{% macro starrocks__json_extract_json(json_col, json_path) -%}
    {# StarRocks: json_extract does not exist; use json_query on a parsed JSON value #}
    json_query(parse_json({{ json_col }}), {{ json_path }})
{%- endmacro %}


{% macro from_iso8601_timestamp(timestamp_str) -%}
    {{ adapter.dispatch('from_iso8601_timestamp', 'open_learning')(timestamp_str) }}
{%- endmacro %}

{% macro default__from_iso8601_timestamp(timestamp_str) -%}
    {#
        Trino: from_iso8601_timestamp() without a timezone offset in the string uses the
        session timezone, causing "Illegal instant due to time zone offset transition" for
        timestamps in DST gap hours (e.g. 2:xx AM on spring-forward day in America/New_York).

      Behavior of this wrapper:
      * Strings that already carry a timezone marker at the end
        (ending in 'Z', 'z', '+HH:MM', or '-HH:MM') are passed through unchanged.
      * Plain dates in 'YYYY-MM-DD' format are treated as midnight UTC by
        appending 'T00:00:00Z'.
      * All other strings (naive datetimes with no explicit zone) have 'Z'
        appended so Trino parses them as UTC, avoiding DST-gap issues.
    #}
    from_iso8601_timestamp(
        case
            when regexp_like({{ timestamp_str }}, '.*([Zz]|[+-][0-9]{2}:[0-9]{2})$')
              then {{ timestamp_str }}
            when regexp_like({{ timestamp_str }}, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then
                {{ timestamp_str }} || 'T00:00:00Z'
            else {{ timestamp_str }} || 'Z'
        end
    )
{%- endmacro %}

{% macro duckdb__from_iso8601_timestamp(timestamp_str) -%}
    {# DuckDB: use strptime or cast #}
    cast({{ timestamp_str }} as timestamp)
{%- endmacro %}

{% macro starrocks__from_iso8601_timestamp(timestamp_str) -%}
    {#
        StarRocks: DATETIME is zone-less, and a bare cast() returns NULL for strings with
        a trailing 'Z' or '+HH:MM'/'-HH:MM' offset. Strip any such suffix and the 'T'
        separator before casting. Truncate to 26 chars to keep at most microsecond
        precision (and avoid cast() failures on nanosecond-precision inputs). Non-UTC
        offsets are dropped rather than applied, which is acceptable since this project's
        source data is UTC/naive.
    #}
    cast(
        substr(
            replace(regexp_replace({{ timestamp_str }}, '([Zz]|[+-][0-9]{2}:?[0-9]{2})$', ''), 'T', ' ')
            , 1, 26
        ) as datetime
    )
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

{% macro starrocks__from_iso8601_timestamp_nanos(timestamp_str) -%}
    {# StarRocks: max precision is microseconds (v3.3.5+); strip timezone marker and truncate to 6 fractional digits #}
    cast(substr(replace(regexp_replace({{ timestamp_str }}, '([Zz]|[+-][0-9]{2}:?[0-9]{2})$', ''), 'T', ' '), 1, 26) as datetime)
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
    {# StarRocks: native support, including the 3rd null_replace_str arg #}
    {% if null_replacement %}
        array_join({{ array_expr }}, '{{ delimiter }}', '{{ null_replacement }}')
    {% else %}
        array_join({{ array_expr }}, '{{ delimiter }}')
    {% endif %}
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
    {{ string_expr }} regexp {{ starrocks_string_literal(pattern) }}
{%- endmacro %}


{#
    mongo_objectid_timestamp: the creation time a Mongo ObjectId encodes in its first
    8 hex characters (Unix seconds), as a timestamp.
#}
{% macro mongo_objectid_timestamp(objectid_expr) -%}
    {{ adapter.dispatch('mongo_objectid_timestamp', 'open_learning')(objectid_expr) }}
{%- endmacro %}

{% macro default__mongo_objectid_timestamp(objectid_expr) -%}
    from_unixtime(from_base(substr({{ objectid_expr }}, 1, 8), 16))
{%- endmacro %}

{% macro duckdb__mongo_objectid_timestamp(objectid_expr) -%}
    to_timestamp(cast('0x' || substr({{ objectid_expr }}, 1, 8) as bigint))
{%- endmacro %}

{% macro starrocks__mongo_objectid_timestamp(objectid_expr) -%}
    cast(from_unixtime(cast(conv(substr({{ objectid_expr }}, 1, 8), 16, 10) as bigint)) as datetime)
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

{% macro starrocks__format_datetime(datetime_expr, java_format) -%}
    {# StarRocks: jodatime_format accepts Java DateTime patterns directly (v3.1+), no conversion needed #}
    jodatime_format({{ datetime_expr }}, '{{ java_format }}')
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

{% macro starrocks__date_format(datetime_expr, format_string) -%}
    {# StarRocks: date_format uses the same MySQL-style format language as Trino date_format #}
    date_format({{ datetime_expr }}, {{ format_string }})
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

{% macro starrocks__day_of_week(date_expr) -%}
    {# StarRocks: dayofweek_iso returns 1=Mon, 7=Sun. Plain dayofweek() is 1=Sun — do not use. #}
    dayofweek_iso({{ date_expr }})
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

{% macro starrocks__iso8601_to_date_key(varchar_field) -%}
    {# StarRocks: str_to_date returns NULL on parse failure, no try wrapper needed #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(date_format(str_to_date({{ varchar_field }}, '%Y-%m-%d'), '%Y%m%d') AS INT)
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(date_format(str_to_date(SUBSTR({{ varchar_field }}, 1, 10), '%Y-%m-%d'), '%Y%m%d') AS INT)
        ELSE NULL
    END
{%- endmacro %}

{#
    iso8601_to_time_key: Convert an ISO8601 varchar datetime field to an integer HHMM time key.
    Matches the dim_time.time_key format (hour * 100 + minute).
    Handles strings of the form 'YYYY-MM-DDTHH:MM:SS...' (>=16 chars).
    Returns NULL if input is NULL or shorter than 16 characters.
#}
{% macro iso8601_to_time_key(varchar_field) -%}
    {{ return(adapter.dispatch('iso8601_to_time_key', 'open_learning')(varchar_field)) }}
{%- endmacro %}

{% macro default__iso8601_to_time_key(varchar_field) -%}
    {# Trino: substr on ISO 8601 string positions 12-13 (HH) and 15-16 (MM) #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) >= 16 THEN
            CAST(SUBSTR({{ varchar_field }}, 12, 2) AS INTEGER) * 100
            + CAST(SUBSTR({{ varchar_field }}, 15, 2) AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}

{% macro duckdb__iso8601_to_time_key(varchar_field) -%}
    {# DuckDB: identical string-based extraction #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) >= 16 THEN
            CAST(SUBSTR({{ varchar_field }}, 12, 2) AS INTEGER) * 100
            + CAST(SUBSTR({{ varchar_field }}, 15, 2) AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}

{% macro starrocks__iso8601_to_time_key(varchar_field) -%}
    {# StarRocks: identical string-based extraction #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) >= 16 THEN
            CAST(SUBSTR({{ varchar_field }}, 12, 2) AS INTEGER) * 100
            + CAST(SUBSTR({{ varchar_field }}, 15, 2) AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}


{#
    last_value_ignore_nulls: Cross-db wrapper for last_value with IGNORE NULLS.
    Trino: last_value(expr) IGNORE NULLS OVER (window)
    DuckDB: last_value(expr IGNORE NULLS) OVER (window)
    StarRocks: last_value(expr IGNORE NULLS) OVER (window) (v2.5+)

    Usage (write the OVER clause inline after the macro call):
      {{ last_value_ignore_nulls('my_expr') }} over (partition by ... order by ...)

    NOTE: StarRocks' default window frame is ROWS, while Trino's is RANGE.
    Call sites that rely on the default frame must specify an explicit
    frame (e.g. "rows between unbounded preceding and current row") to get
    matching results across engines.
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

{% macro starrocks__last_value_ignore_nulls(expr) -%}
    {# StarRocks: IGNORE NULLS sits inside the function arguments, like DuckDB (v2.5+) #}
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

{% macro starrocks__unnest_json_map(json_expr, alias, key_col, val_col) -%}
    {#
        StarRocks: json_each() has FIXED output column names ('key', 'value') that
        cannot be aliased, unlike Trino/DuckDB's UNNEST. Callers must pass
        key_col='key' and val_col='value' literally; this macro renames nothing,
        it only validates the contract and lets the call site re-alias downstream
        (e.g. `select t.value as topic`). The value column is JSON-typed, so
        downstream consumers need to cast it to varchar.
    #}
    {%- if key_col != 'key' or val_col != 'value' -%}
        {{ exceptions.raise_compiler_error(
            "starrocks__unnest_json_map requires key_col='key' and val_col='value' "
            ~ "(StarRocks json_each() output column names are fixed and cannot be "
            ~ "aliased); got key_col='" ~ key_col ~ "', val_col='" ~ val_col ~ "'."
        ) }}
    {%- endif -%}
    lateral json_each(parse_json(cast({{ json_expr }} as varchar))) {{ alias }}
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

{% macro starrocks__json_is_object(json_expr) -%}
    {# StarRocks: no dedicated json_type(); a JSON object's cast-to-varchar text starts with '{' #}
    substr(cast({{ json_expr }} as varchar), 1, 1) = '{'
{%- endmacro %}


{#
    unnest_json_array: Cross-db unnesting of a JSON array into individual (json) rows.
    Trino: UNNEST(try_cast(json_parse(expr) as array(json))) AS alias(col)
    DuckDB: UNNEST(try_cast(expr as json[])) AS alias(col)

    Usage (in FROM / CROSS JOIN clause):
      cross join {{ unnest_json_array('col_expr', 't', 'element') }}

    Parameters:
      json_expr: expression (varchar) containing a JSON array string
      alias:     table alias for the result
      col_name:  column name for each array element
#}
{#
    unnest_regexp_matches: one row per match of `pattern` in `string_expr`, holding
    the pattern's first capture group. Trino and DuckDB both spell this
    unnest(regexp_extract_all(s, p, 1)), so it has no per-adapter bodies.
#}
{% macro unnest_regexp_matches(string_expr, pattern, alias, col_name) -%}
    unnest({{ regexp_extract_all(string_expr, pattern, 1) }}) as {{ alias }} ({{ col_name }})
{%- endmacro %}


{% macro unnest_json_array(json_expr, alias, col_name) -%}
    {{ adapter.dispatch('unnest_json_array', 'open_learning')(json_expr, alias, col_name) }}
{%- endmacro %}

{% macro default__unnest_json_array(json_expr, alias, col_name) -%}
    {# UNNEST skips NULL inputs, yielding 0 rows for malformed/non-array input. #}
    unnest({{ try_parse_json_array(json_expr) }}) as {{ alias }} ({{ col_name }})
{%- endmacro %}

{#
    try_parse_json_array: a varchar JSON array string as an array of JSON values, or
    NULL where unnest_json_array would yield no rows. The json_array test uses it to
    report the rows unnest_json_array drops.
#}
{% macro try_parse_json_array(json_expr) -%}
    {{ adapter.dispatch('try_parse_json_array', 'open_learning')(json_expr) }}
{%- endmacro %}

{% macro default__try_parse_json_array(json_expr) -%}
    {#
        Trino: try() wraps json_parse() so malformed JSON returns NULL before try_cast
        sees it (json_parse raises before try_cast can catch). try_cast then converts
        the JSON value to array(json), returning NULL for non-array JSON.
    #}
    try_cast(try(json_parse({{ json_expr }})) as array(json))
{%- endmacro %}

{% macro duckdb__try_parse_json_array(json_expr) -%}
    {# DuckDB: cast the varchar JSON array string directly to json[] (list of json values). #}
    try_cast({{ json_expr }} as json[])
{%- endmacro %}

{% macro starrocks__try_parse_json_array(json_expr) -%}
    {# StarRocks: parse the JSON string, then cast to an array of JSON elements #}
    cast(parse_json({{ json_expr }}) as array<json>)
{%- endmacro %}

{#
    json_extract_varchar_array: Extract a JSON array field and cast to an array of varchar
    for use with array_join(). Handles the Trino/DuckDB difference in JSON-to-array casting.

    Trino: json_parse(json_query(col, 'lax $.path')) cast to array(varchar)
    DuckDB: json_extract(col, '$.path') cast to varchar[]

    Usage:
      {{ array_join(json_extract_varchar_array('metadata', "'$.level'"), ', ') }}
#}
{% macro json_extract_varchar_array(json_col, json_path) -%}
    {{ adapter.dispatch('json_extract_varchar_array', 'open_learning')(json_col, json_path) }}
{%- endmacro %}

{% macro default__json_extract_varchar_array(json_col, json_path) -%}
    cast(json_parse(json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}')) as array(varchar))
{%- endmacro %}

{% macro duckdb__json_extract_varchar_array(json_col, json_path) -%}
    cast(json_extract({{ json_col }}, {{ json_path }}) as varchar[])
{%- endmacro %}

{% macro starrocks__json_extract_varchar_array(json_col, json_path) -%}
    {# StarRocks: json_query returns a JSON value; array casts require v2.4+ #}
    cast(json_query(parse_json({{ json_col }}), {{ json_path }}) as array<varchar>)
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

{% macro starrocks__is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
    {# StarRocks: str_to_date returns NULL on parse failure, no try wrapper needed #}
    case
        when
            str_to_date(substr({{ start_on_timestamp_str }}, 1, 10), '%Y-%m-%d') <= current_date
            and (
                {{ end_on_timestamp_str }} is null
                or str_to_date(substr({{ end_on_timestamp_str }}, 1, 10), '%Y-%m-%d') >= current_date
            )
        then true
        else false
    end
{%- endmacro %}


{% macro null_double_array() -%}
    {{ adapter.dispatch('null_double_array', 'open_learning')() }}
{%- endmacro %}

{% macro default__null_double_array() -%}cast(null as array(double)){%- endmacro %}

{% macro duckdb__null_double_array() -%}cast(null as double[]){%- endmacro %}

{% macro starrocks__null_double_array() -%}cast(null as array<double>){%- endmacro %}

{# Trino's array(double) syntax is rejected by DuckDB's parser (double[] there) --
   same per-adapter split as null_double_array, for casting a real column instead
   of a literal null. #}
{% macro cast_double_array(column_name) -%}
    {{ adapter.dispatch('cast_double_array', 'open_learning')(column_name) }}
{%- endmacro %}

{% macro default__cast_double_array(column_name) -%}cast({{ column_name }} as array(double)){%- endmacro %}

{% macro duckdb__cast_double_array(column_name) -%}cast({{ column_name }} as double[]){%- endmacro %}

{% macro starrocks__cast_double_array(column_name) -%}cast({{ column_name }} as array<double>){%- endmacro %}

{% macro null_varchar_array() -%}
    {{ adapter.dispatch('null_varchar_array', 'open_learning')() }}
{%- endmacro %}

{% macro default__null_varchar_array() -%}cast(null as array(varchar)){%- endmacro %}

{% macro duckdb__null_varchar_array() -%}cast(null as varchar[]){%- endmacro %}

{% macro starrocks__null_varchar_array() -%}cast(null as array<varchar>){%- endmacro %}


{% macro empty_varchar_array() -%}
    {{ adapter.dispatch('empty_varchar_array', 'open_learning')() }}
{%- endmacro %}

{% macro default__empty_varchar_array() -%}cast(array[] as array(varchar)){%- endmacro %}

{% macro duckdb__empty_varchar_array() -%}cast([] as varchar[]){%- endmacro %}

{% macro starrocks__empty_varchar_array() -%}cast([] as array<varchar>){%- endmacro %}


{#
    array_of: an array of the given SQL expressions. StarRocks has no array[...]
    constructor, only the bare bracket form.
#}
{% macro array_of(elements) -%}
    {{ adapter.dispatch('array_of', 'open_learning')(elements) }}
{%- endmacro %}

{% macro default__array_of(elements) -%}
    array[{{ elements | join(', ') }}]
{%- endmacro %}

{% macro starrocks__array_of(elements) -%}
    [{{ elements | join(', ') }}]
{%- endmacro %}


{% macro array_length(array_expr) -%}
    {{ adapter.dispatch('array_length', 'open_learning')(array_expr) }}
{%- endmacro %}

{% macro default__array_length(array_expr) -%}
    cardinality({{ array_expr }})
{%- endmacro %}

{% macro duckdb__array_length(array_expr) -%}
    {# DuckDB's cardinality() only accepts maps #}
    len({{ array_expr }})
{%- endmacro %}

{% macro starrocks__array_length(array_expr) -%}
    array_length({{ array_expr }})
{%- endmacro %}


{#
    array_filter_nonempty: drop NULL and empty-string elements from a varchar array,
    keeping the order of the rest.
#}
{% macro array_filter_nonempty(array_expr) -%}
    {{ adapter.dispatch('array_filter_nonempty', 'open_learning')(array_expr) }}
{%- endmacro %}

{% macro default__array_filter_nonempty(array_expr) -%}
    filter({{ array_expr }}, x -> x is not null and x != '')
{%- endmacro %}

{% macro duckdb__array_filter_nonempty(array_expr) -%}
    list_filter({{ array_expr }}, x -> x is not null and x != '')
{%- endmacro %}

{% macro starrocks__array_filter_nonempty(array_expr) -%}
    array_filter({{ array_expr }}, x -> x is not null and x != '')
{%- endmacro %}


{#
    unnest_sequence: one row per integer 1..length_expr, for walking several arrays
    in step with element_at_array(). length_expr must be >= 1: Trino's
    sequence(1, 0) counts down rather than returning an empty array.
#}
{% macro unnest_sequence(length_expr, alias, col_name) -%}
    {{ adapter.dispatch('unnest_sequence', 'open_learning')(length_expr, alias, col_name) }}
{%- endmacro %}

{% macro default__unnest_sequence(length_expr, alias, col_name) -%}
    unnest(sequence(1, {{ length_expr }})) as {{ alias }} ({{ col_name }})
{%- endmacro %}

{% macro duckdb__unnest_sequence(length_expr, alias, col_name) -%}
    unnest(generate_series(1, {{ length_expr }})) as {{ alias }} ({{ col_name }})
{%- endmacro %}

{% macro starrocks__unnest_sequence(length_expr, alias, col_name) -%}
    {# array_generate needs its step spelled out when the bound is a column. #}
    unnest(array_generate(1, {{ length_expr }}, 1)) as {{ alias }} ({{ col_name }})
{%- endmacro %}


{#
    try_cast: `expr` as `type`, or NULL where the value does not convert. StarRocks
    has no try_cast; its cast already returns NULL for a string that does not convert.
#}
{% macro try_cast(expr, type) -%}
    {{ adapter.dispatch('try_cast', 'open_learning')(expr, type) }}
{%- endmacro %}

{% macro default__try_cast(expr, type) -%}
    try_cast({{ expr }} as {{ type }})
{%- endmacro %}

{% macro starrocks__try_cast(expr, type) -%}
    cast({{ expr }} as {{ type }})
{%- endmacro %}


{#
    try_or_null: `expr`, or NULL where evaluating it raises. StarRocks has no try();
    only wrap expressions whose StarRocks form already returns NULL on bad input
    (date_parse, which is str_to_date there).
#}
{% macro try_or_null(expr) -%}
    {{ adapter.dispatch('try_or_null', 'open_learning')(expr) }}
{%- endmacro %}

{% macro default__try_or_null(expr) -%}
    try({{ expr }})
{%- endmacro %}

{% macro starrocks__try_or_null(expr) -%}
    {{ expr }}
{%- endmacro %}


{#
    codepoint_char: the one-character string for a code point, e.g. 10 -> a newline.
    StarRocks has char() and no chr().
#}
{% macro codepoint_char(codepoint) -%}
    {{ adapter.dispatch('codepoint_char', 'open_learning')(codepoint) }}
{%- endmacro %}

{% macro default__codepoint_char(codepoint) -%}
    chr({{ codepoint }})
{%- endmacro %}

{% macro starrocks__codepoint_char(codepoint) -%}
    char({{ codepoint }})
{%- endmacro %}


{#
    regexp_replace_all: replace every match. DuckDB's regexp_replace replaces only the
    first match unless given the 'g' option; Trino's always replaces all.
#}
{% macro regexp_replace_all(subject, pattern, replacement) -%}
    {{ adapter.dispatch('regexp_replace_all', 'open_learning')(subject, pattern, replacement) }}
{%- endmacro %}

{% macro regexp_split(subject, pattern) -%}
    {{ adapter.dispatch('regexp_split', 'open_learning')(subject, pattern) }}
{%- endmacro %}

{% macro default__regexp_split(subject, pattern) -%}
    regexp_split({{ subject }}, {{ pattern }})
{%- endmacro %}

{% macro duckdb__regexp_split(subject, pattern) -%}
    string_split_regex({{ subject }}, {{ pattern }})
{%- endmacro %}

{% macro starrocks__regexp_split(subject, pattern) -%}
    regexp_split({{ subject }}, {{ starrocks_string_literal(pattern) }})
{%- endmacro %}


{% macro default__regexp_replace_all(subject, pattern, replacement) -%}
    regexp_replace({{ subject }}, {{ pattern }}, {{ replacement }})
{%- endmacro %}

{% macro duckdb__regexp_replace_all(subject, pattern, replacement) -%}
    regexp_replace({{ subject }}, {{ pattern }}, {{ replacement }}, 'g')
{%- endmacro %}

{% macro starrocks__regexp_replace_all(subject, pattern, replacement) -%}
    regexp_replace({{ subject }}, {{ starrocks_string_literal(pattern) }}, {{ replacement }})
{%- endmacro %}


{#
    strip_whitespace: `string_expr` without its leading and trailing whitespace of any
    kind (Trino's trim() removes only spaces). Two passes, because StarRocks 4.1.6
    applies only the first branch of '^\s+|\s+$'.
#}
{% macro strip_whitespace(string_expr) -%}
    {{ regexp_replace_all(regexp_replace_all(string_expr, "'^\\s+'", "''"), "'\\s+$'", "''") }}
{%- endmacro %}


{#
    starrocks_string_literal: a quoted SQL literal written for Trino or DuckDB, as
    StarRocks reads it. StarRocks treats a backslash in a string literal as an escape
    character, so '\d' reaches its regex engine as 'd'. Every starrocks__ macro that
    takes a pattern passes it through here.
#}
{% macro starrocks_string_literal(literal) -%}
    {{ literal | replace('\\', '\\\\') }}
{%- endmacro %}


{#
    regexp_extract_all: every match of `pattern` in `subject` (or of its capture group
    `group`), as an array of varchar.
#}
{% macro regexp_extract_all(subject, pattern, group=none) -%}
    {{ adapter.dispatch('regexp_extract_all', 'open_learning')(subject, pattern, group) }}
{%- endmacro %}

{% macro default__regexp_extract_all(subject, pattern, group=none) -%}
    regexp_extract_all({{ subject }}, {{ pattern }}{% if group is not none %}, {{ group }}{% endif %})
{%- endmacro %}

{% macro starrocks__regexp_extract_all(subject, pattern, group=none) -%}
    regexp_extract_all({{ subject }}, {{ starrocks_string_literal(pattern) }}, {{ group if group is not none else 0 }})
{%- endmacro %}


{#
    local_date_to_timestamptz: midnight of a YYYY-MM-DD date string in `time_zone`, as a
    zone-aware timestamp. Compare it with current_timestamp and render it with
    format_timestamp_as_iso8601 on either engine.
#}
{% macro local_date_to_timestamptz(date_expr, time_zone) -%}
    {{ adapter.dispatch('local_date_to_timestamptz', 'open_learning')(date_expr, time_zone) }}
{%- endmacro %}

{% macro default__local_date_to_timestamptz(date_expr, time_zone) -%}
    with_timezone(cast(cast({{ date_expr }} as date) as timestamp), '{{ time_zone }}')
{%- endmacro %}

{% macro duckdb__local_date_to_timestamptz(date_expr, time_zone) -%}
    timezone('{{ time_zone }}', cast(cast({{ date_expr }} as date) as timestamp))
{%- endmacro %}

{# StarRocks has no zone-aware type. An instant is a DATETIME holding UTC wall-clock time. #}
{% macro starrocks__local_date_to_timestamptz(date_expr, time_zone) -%}
    convert_tz(cast(cast({{ date_expr }} as date) as datetime), '{{ time_zone }}', 'UTC')
{%- endmacro %}


{#
    timestamptz_at_utc: the same instant with its zone set to UTC, so that
    format_timestamp_as_iso8601 renders it with a Z on Trino too (Trino's to_iso8601
    keeps the value's zone). DuckDB's formatter already normalizes to UTC.
#}
{% macro timestamptz_at_utc(timestamp_expr) -%}
    {{ adapter.dispatch('timestamptz_at_utc', 'open_learning')(timestamp_expr) }}
{%- endmacro %}

{% macro default__timestamptz_at_utc(timestamp_expr) -%}
    at_timezone({{ timestamp_expr }}, 'UTC')
{%- endmacro %}

{% macro duckdb__timestamptz_at_utc(timestamp_expr) -%}
    {{ timestamp_expr }}
{%- endmacro %}

{% macro starrocks__timestamptz_at_utc(timestamp_expr) -%}
    {{ timestamp_expr }}
{%- endmacro %}


{#
    local_timestamp_to_timestamptz: a wall-clock timestamp read in the zone that
    `zone_expr` evaluates to, as a zone-aware timestamp. Unlike
    local_date_to_timestamptz the zone is a SQL expression, so it can vary by row.
#}
{% macro local_timestamp_to_timestamptz(timestamp_expr, zone_expr) -%}
    {{ adapter.dispatch('local_timestamp_to_timestamptz', 'open_learning')(timestamp_expr, zone_expr) }}
{%- endmacro %}

{% macro default__local_timestamp_to_timestamptz(timestamp_expr, zone_expr) -%}
    with_timezone(cast({{ timestamp_expr }} as timestamp), {{ zone_expr }})
{%- endmacro %}

{% macro duckdb__local_timestamp_to_timestamptz(timestamp_expr, zone_expr) -%}
    timezone({{ zone_expr }}, cast({{ timestamp_expr }} as timestamp))
{%- endmacro %}

{% macro starrocks__local_timestamp_to_timestamptz(timestamp_expr, zone_expr) -%}
    convert_tz(cast({{ timestamp_expr }} as datetime), {{ zone_expr }}, 'UTC')
{%- endmacro %}


{#
    null_timestamptz / current_timestamptz: a NULL and the current instant, of the
    type the *_to_timestamptz macros return, so they union and compare with those
    values on every engine. StarRocks' current_timestamp is session-local wall-clock
    time, which is not comparable with its UTC DATETIME instants.
#}
{% macro null_timestamptz() -%}
    {{ adapter.dispatch('null_timestamptz', 'open_learning')() }}
{%- endmacro %}

{% macro default__null_timestamptz() -%}cast(null as timestamp with time zone){%- endmacro %}

{% macro starrocks__null_timestamptz() -%}cast(null as datetime){%- endmacro %}

{% macro current_timestamptz() -%}
    {{ adapter.dispatch('current_timestamptz', 'open_learning')() }}
{%- endmacro %}

{% macro default__current_timestamptz() -%}current_timestamp{%- endmacro %}

{% macro starrocks__current_timestamptz() -%}utc_timestamp(){%- endmacro %}


{#
    md5_hex: lowercase hex MD5 of a string's UTF-8 bytes, as Python's
    hashlib.md5(s.encode()).hexdigest() gives it.
#}
{% macro md5_hex(string_expr) -%}
    {{ adapter.dispatch('md5_hex', 'open_learning')(string_expr) }}
{%- endmacro %}

{% macro default__md5_hex(string_expr) -%}
    lower(to_hex(md5(to_utf8({{ string_expr }}))))
{%- endmacro %}

{% macro duckdb__md5_hex(string_expr) -%}
    md5({{ string_expr }})
{%- endmacro %}

{% macro starrocks__md5_hex(string_expr) -%}
    md5({{ string_expr }})
{%- endmacro %}


{#
    title_case: Python's str.title(). A letter is upper-cased when it starts the
    string or follows a non-letter, and lower-cased otherwise, so digits and
    punctuation start a new word too: "l3.1x intro" -> "L3.1X Intro". Built
    character by character because neither engine has a case-changing regex
    replacement that the other shares.
#}
{% macro title_case(string_expr) -%}
    {{ adapter.dispatch('title_case', 'open_learning')(string_expr) }}
{%- endmacro %}

{% macro default__title_case(string_expr) -%}
    case when {{ string_expr }} = '' then '' else array_join(
        transform(
            sequence(1, length({{ string_expr }}))
            , i -> case
                when i = 1 or not regexp_like(substr({{ string_expr }}, i - 1, 1), '\p{L}')
                    then upper(substr({{ string_expr }}, i, 1))
                else lower(substr({{ string_expr }}, i, 1))
            end
        )
        , ''
    ) end
{%- endmacro %}

{% macro duckdb__title_case(string_expr) -%}
    case when {{ string_expr }} = '' then '' else array_to_string(
        list_transform(
            range(1, length({{ string_expr }}) + 1)
            , i -> case
                when i = 1 or not regexp_matches(substr({{ string_expr }}, i - 1, 1), '\p{L}')
                    then upper(substr({{ string_expr }}, i, 1))
                else lower(substr({{ string_expr }}, i, 1))
            end
        )
        , ''
    ) end
{%- endmacro %}

{# StarRocks' length() counts bytes; substr() counts characters, as char_length() does. #}
{% macro starrocks__title_case(string_expr) -%}
    case when {{ string_expr }} = '' then '' else array_join(
        array_map(
            i -> case
                when i = 1 or not (substr({{ string_expr }}, i - 1, 1) regexp '\\p{L}')
                    then upper(substr({{ string_expr }}, i, 1))
                else lower(substr({{ string_expr }}, i, 1))
            end
            , array_generate(1, char_length({{ string_expr }}))
        )
        , ''
    ) end
{%- endmacro %}


{#
    html_unescape: decode the HTML entities upstream feeds put in plain-text fields.
    Covers the entities the MIT PE feed has been seen to use, not every entity Python's
    html.unescape knows; tests/assert_mitpe_text_has_no_html_entities.sql warns on any
    other. &amp; goes last so an escaped entity (&amp;lt;) decodes once.
#}
{% macro html_unescape(string_expr) -%}
    replace(replace(replace(replace(replace(replace(replace(
        {{ string_expr }}
        , '&#039;', ''''), '&#39;', ''''), '&apos;', ''''), '&quot;', '"'), '&lt;', '<')
        , '&gt;', '>'), '&amp;', '&')
{%- endmacro %}


{#
    json_array_field_values: the values of `field` in each object of a JSON array
    string, as an array of `element_type`. '[{"name": "a"}, {"name": "b"}]' -> ['a', 'b'].
#}
{% macro json_array_field_values(json_col, field, element_type='varchar') -%}
    {{ adapter.dispatch('json_array_field_values', 'open_learning')(json_col, field, element_type) }}
{%- endmacro %}

{% macro default__json_array_field_values(json_col, field, element_type='varchar') -%}
    cast(json_parse(json_query({{ json_col }}, 'lax $.{{ field }}' with array wrapper)) as array({{ element_type }}))  --noqa
{%- endmacro %}

{% macro duckdb__json_array_field_values(json_col, field, element_type='varchar') -%}
    cast(json_extract_string({{ json_col }}, '$[*].{{ field }}') as {{ element_type }}[])
{%- endmacro %}

{% macro starrocks__json_array_field_values(json_col, field, element_type='varchar') -%}
    cast(json_query(parse_json({{ json_col }}), '$[*].{{ field }}') as array<{{ element_type }}>)
{%- endmacro %}


{#
    json_array_string: the JSON array at `json_path` inside a JSON value, as a varchar
    JSON string, so it can be passed to unnest_json_array.
#}
{% macro json_array_string(json_col, json_path) -%}
    {{ adapter.dispatch('json_array_string', 'open_learning')(json_col, json_path) }}
{%- endmacro %}

{% macro default__json_array_string(json_col, json_path) -%}
    json_format(json_extract({{ json_col }}, {{ json_path }}))
{%- endmacro %}

{% macro duckdb__json_array_string(json_col, json_path) -%}
    cast(json_extract({{ json_col }}, {{ json_path }}) as varchar)
{%- endmacro %}

{% macro starrocks__json_array_string(json_col, json_path) -%}
    {# json_col may be a varchar or a JSON value (an unnest_json_array element); the cast
       to varchar makes parse_json accept either. #}
    cast(json_query(parse_json(cast({{ json_col }} as varchar)), {{ json_path }}) as varchar)
{%- endmacro %}


{#
    json_nested_array_distinct_values: the distinct strings in a JSON array of arrays
    at `json_path`, sorted. '[["b", "a"], ["a"]]' -> ['a', 'b']. An absent path gives
    an empty array.
#}
{% macro json_nested_array_distinct_values(json_col, json_path) -%}
    coalesce(
        array_sort(array_distinct({{ 'array_flatten' if target.type == 'starrocks' else 'flatten' }}(
            {{ adapter.dispatch('json_extract_nested_varchar_array', 'open_learning')(json_col, json_path) }}
        )))
        , {{ empty_varchar_array() }}
    )
{%- endmacro %}

{% macro default__json_extract_nested_varchar_array(json_col, json_path) -%}
    cast(json_parse(json_query({{ json_col }}, 'lax {{ json_path | replace("'", "") }}')) as array(array(varchar)))
{%- endmacro %}

{% macro duckdb__json_extract_nested_varchar_array(json_col, json_path) -%}
    cast(json_extract({{ json_col }}, {{ json_path }}) as varchar[][])
{%- endmacro %}

{% macro starrocks__json_extract_nested_varchar_array(json_col, json_path) -%}
    {# StarRocks refuses a cast from JSON straight to array<array<varchar>>. #}
    array_map(
        inner_array -> cast(inner_array as array<varchar>)
        , cast(json_query(parse_json({{ json_col }}), {{ json_path }}) as array<json>)
    )
{%- endmacro %}

{# base64url_decode_or_null: URL-safe base64 to UTF-8 text, NULL when it does not decode. #}
{% macro base64url_decode_or_null(string_expr) -%}
    {{ adapter.dispatch('base64url_decode_or_null', 'open_learning')(string_expr) }}
{%- endmacro %}

{% macro default__base64url_decode_or_null(string_expr) -%}
    try(from_utf8(from_base64url({{ string_expr }})))
{%- endmacro %}

{% macro duckdb__base64url_decode_or_null(string_expr) -%}
    {# DuckDB's from_base64 takes only the standard alphabet, padded. #}
    try(decode(from_base64(rpad(
        translate({{ string_expr }}, '-_', '+/')
        , cast(ceil(length({{ string_expr }}) / 4.0) * 4 as integer)
        , '='
    ))))
{%- endmacro %}


{# json_object_from_pairs: a JSON object as varchar from [key, expression] pairs, in order. #}
{% macro json_object_from_pairs(pairs) -%}
    {{ adapter.dispatch('json_object_from_pairs', 'open_learning')(pairs) }}
{%- endmacro %}

{% macro default__json_object_from_pairs(pairs) -%}
    json_object(
        {%- for key, expr in pairs %}
        {% if not loop.first %}, {% endif %}'{{ key }}': {{ expr }}
        {%- endfor %}
    )
{%- endmacro %}

{% macro duckdb__json_object_from_pairs(pairs) -%}
    cast(json_object(
        {%- for key, expr in pairs %}
        {% if not loop.first %}, {% endif %}'{{ key }}', {{ expr }}
        {%- endfor %}
    ) as varchar)
{%- endmacro %}
