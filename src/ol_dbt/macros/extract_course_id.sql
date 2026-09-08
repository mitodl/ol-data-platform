{% macro extract_course_id_from_tracking_log(course_id_has_old_format=false) %}
    ---course ID format: {key type}:{org}+{course number}+{run tag} for courses created since Fall 2014
    ---course ID format: {org}/{course number}/{run tag} for courses created before Fall 2014
    ---Course number and run tag can be letters, numbers, period, dashes, underscores
    {% if course_id_has_old_format %}
    {% set course_id_regex = '(([\w\.\-\_]+):([\w\.\-\_]+)[+]([\w\.\-\_]+)[+]([\w\.\-\_]+))|(([\w\.\-\_]+)[/]([\w\.\-\_]+)[/]([\w\.\-\_]+))' %}
    {% else %}
    {% set course_id_regex = 'course-v(\d{1}):([\w\.\-\_]+)\+([\w\.\-\_]+)\+([\w\.\-\_]+)' %}
{% endif %}

    {#- Routed through json_query_string and regexp_extract_or_null so this compiles and
        behaves the same on Trino and DuckDB. Two engine gaps are in play:

        1. Trino's `json_query(context, 'lax $.x' omit quotes)` is a parse error on DuckDB
           (`syntax error at or near "omit"`). json_query_string exists for exactly this
           substitution and its Trino branch emits that same expression unchanged, so the
           Trino-side SQL here is untouched. It is also what the calling tracking-log
           models already use for $.user_id / $.org_id / $.path.
        2. DuckDB's regexp_extract returns '' rather than NULL on no match. A missing JSON
           key is still safe (NULL in, NULL out), but a key that is *present and does not
           match* the course-id pattern yields '', which `is not null` accepts -- so that
           arm would win and emit the non-course-id string verbatim. `nullif(..., '')`
           restores Trino's fall-through. -#}
    {%- set course_id_pattern = "'" ~ course_id_regex ~ "'" -%}
    {%- set context_course_id = json_query_string('context', "'$.course_id'") -%}
    {%- set context_path = json_query_string('context', "'$.path'") -%}

      case
          when {{ regexp_extract_or_null(context_course_id, course_id_pattern) }} is not null
             then {{ context_course_id }}
          when {{ regexp_extract_or_null(context_path, course_id_pattern) }} is not null
              then {{ regexp_extract_or_null(context_path, course_id_pattern) }}
          when {{ regexp_extract_or_null('event_type', course_id_pattern) }} is not null
              then {{ regexp_extract_or_null('event_type', course_id_pattern) }}
          when {{ regexp_extract_or_null('page', course_id_pattern) }} is not null
              then {{ regexp_extract_or_null('page', course_id_pattern) }}
      end
{% endmacro %}


{% macro extract_course_readable_id(courserun_readable_id) %}
    ---Output: course_readable_id in course-v1:{org}+{course number} format
    ---Input: courserun_readable_id in course-v1:{org}+{course number}+{run tag} for courses created since Fall 2014,
    --- {org}/{course number}/{run tag} for courses created before Fall 2014
     case
          when position('course-v' in {{ courserun_readable_id }} ) > 0
             then regexp_extract({{ courserun_readable_id }}, 'course-v(\d{1}):([\w\.\-]+)\+([a-zA-Z0-9.-]+)')
          else
             concat(
                  'course-v1:'
                  , replace(regexp_extract({{ courserun_readable_id }}, '([\w]+)/([a-zA-Z0-9.-]+)'), '/', '+')
             )
      end
{% endmacro %}

--- course IDs come in two formats from different sources. This ensures that course IDs are consistently converted in
--  all the downstream models.
{% macro format_course_id(column_name='courserun_readable_id', convert_to_old_format=true) %}
    {% if convert_to_old_format %}
            -- format as {org}/{course number}/{run}
           replace(replace({{ column_name }}, 'course-v1:', ''), '+', '/')
     {% else %}
           -- format as course-v1:{org}+{course}+{run}
           'course-v1:' || replace({{ column_name }}, '/', '+')
     {% endif %}
{% endmacro %}
