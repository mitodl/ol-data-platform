{% macro is_courserun_current(courserun_start_on, courserun_end_on) %}
   {{ return(adapter.dispatch('is_courserun_current', 'open_learning')(courserun_start_on, courserun_end_on)) }}
{% endmacro %}

{% macro trino__is_courserun_current(courserun_start_on, courserun_end_on) %}
  case
      when
          date(from_iso8601_timestamp({{ courserun_start_on }})) <= current_date
          and
          (
              {{ courserun_end_on }} is null
              or date(from_iso8601_timestamp({{ courserun_end_on }})) >= current_date
          )
      then true
      else false
  end
{% endmacro %}

{% macro duckdb__is_courserun_current(courserun_start_on, courserun_end_on) %}
  case
      when
          date(strptime({{ courserun_start_on }}, '%Y-%m-%dT%H:%M:%S')) <= current_date
          and
          (
              {{ courserun_end_on }} is null
              or date(strptime({{ courserun_end_on }}, '%Y-%m-%dT%H:%M:%S')) >= current_date
          )
      then true
      else false
  end
{% endmacro %}

{% macro default__is_courserun_current(courserun_start_on, courserun_end_on) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__is_courserun_current(courserun_start_on, courserun_end_on)) }}
{% endmacro %}
