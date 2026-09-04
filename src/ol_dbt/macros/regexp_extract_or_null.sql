{% macro regexp_extract_or_null(subject, pattern) %}
  {#
    First substring of `subject` matching `pattern`, or NULL when there is no match.

    The NULL is the point. Trino's regexp_extract already returns NULL on no match, but
    DuckDB's returns an empty string, so callers that branch on `... is not null` (see
    extract_course_id_from_tracking_log) silently take a branch on DuckDB that they would
    skip on Trino. Verified in DuckDB 1.5:

      regexp_extract(NULL, p)              -> NULL   (a missing JSON key stays safe)
      regexp_extract('not-a-course-id', p) -> ''     (`is not null` wrongly accepts this)

    So the divergence bites on a value that is present but doesn't match, not on a missing
    one. This normalizes the no-match result to NULL on every adapter so `is not null`
    means the same thing everywhere.

    Whole-match only -- no capture-group index. The same '' divergence also affects the
    3-arg `regexp_extract(x, p, n)` calls in tfact_chatbot_events.sql (inside coalesce),
    tfact_course_navigation_events.sql and stg__edxorg__s3__program_learner_report.sql.
    Those are outside this PR's diff; add a group parameter here when they get fixed.
  #}
  {{ return(adapter.dispatch('regexp_extract_or_null', 'open_learning')(subject, pattern)) }}
{% endmacro %}

{% macro trino__regexp_extract_or_null(subject, pattern) %}
  {# Trino returns NULL on no match already. #}
  regexp_extract({{ subject }}, {{ pattern }})
{% endmacro %}

{% macro default__regexp_extract_or_null(subject, pattern) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__regexp_extract_or_null(subject, pattern)) }}
{% endmacro %}

{% macro duckdb__regexp_extract_or_null(subject, pattern) %}
  {# DuckDB returns '' (not NULL) when the pattern doesn't match, so map it back.
     https://duckdb.org/docs/stable/sql/functions/regular_expressions #}
  nullif(regexp_extract({{ subject }}, {{ pattern }}), '')
{% endmacro %}

{% macro starrocks__regexp_extract_or_null(subject, pattern) %}
  {# StarRocks requires the group index and, like DuckDB, returns '' on no match.
     Not exercised by any current caller against a live StarRocks target -- verify before relying on it. #}
  nullif(regexp_extract({{ subject }}, {{ pattern }}, 0), '')
{% endmacro %}
