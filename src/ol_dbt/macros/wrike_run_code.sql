{% macro wrike_course_code(run_code, include_partner=true) %}
  {#
    Course part of an Emeritus or Global Alumni Wrike run code, or NULL if the code doesn't
    have the expected shape.

    Run codes look like <partner>-<course code>-<YY>-<MM>#<n>, e.g. MO-DBIP.ELE-25-02#1 (MO is
    Emeritus, MXP is Global Alumni). The course code can carry a variant suffix (.ES, .LITE, ...),
    which is kept: xPro treats variants as separate courses.

      wrike_course_code('MO-DBIP.ELE-25-02#1')         -> 'MO-DBIP.ELE'
      wrike_course_code('MO-DBIP.ELE-25-02#1', false)  -> 'DBIP.ELE'
  #}
  {%- if include_partner -%}
    {{ regexp_extract_or_null(run_code, "'^([A-Z]+-.+)-[0-9]{2}-[0-9]{2}#[0-9]+$'", 1) }}
  {%- else -%}
    {{ regexp_extract_or_null(run_code, "'^[A-Z]+-(.+)-[0-9]{2}-[0-9]{2}#[0-9]+$'", 1) }}
  {%- endif -%}
{% endmacro %}

{% macro wrike_course_codes_of_external_mitxpro_courses() %}
  {#
    One row per (Wrike course code, external xPro course) pair, from the xPro run records that
    carry an Emeritus or Global Alumni run code. Emeritus and Global Alumni have no course table,
    so this is how a run with no xPro run of its own finds its course.

    The code comes with and without its partner prefix. Match on the prefixed code first: xPro
    can hold a separate course per partner for the same code (MO-DL is xPRO+DL, MXP-DL is
    xPRO+DLx). Fall back to the bare code only when the partner has no course of its own, for a
    course that runs through both partners (MO-CRT.ES runs belong to xPRO+CRT-ESP, which xPro
    holds under MXP-CRT.ES).

    A code that maps to more than one xPro course yields more than one row. That is left in on
    purpose: dim_course_run's uniqueness test fails on the fan-out instead of one of the courses
    being picked silently.
  #}
    select distinct
        {{ wrike_course_code('runs.courserun_external_readable_id') }} as wrike_course_code
        , {{ wrike_course_code('runs.courserun_external_readable_id', include_partner=false) }}
        as wrike_course_code_without_partner
        , courses.course_readable_id as mitxpro_course_readable_id
    from {{ ref('int__mitxpro__course_runs') }} as runs
    inner join {{ ref('int__mitxpro__courses') }} as courses
        on runs.course_id = courses.course_id
    where
        courses.course_is_external = true
        and {{ wrike_course_code('runs.courserun_external_readable_id') }} is not null
{% endmacro %}
