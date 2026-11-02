{% macro mitxpro_course_runs_by_external_readable_id() -%}
    {#
      One xPro course run per external run code, as a query to select from. Emeritus and
      Global Alumni enrollments carry that code, and every model that resolves them to an
      xPro run goes through this so they agree on the run.
      xPro can give two runs one code (e.g. MCPO and MO_MCPO both carry MO-MCPO-26-12#1
      since 2026-10-14). The earliest-created run takes the code, so an enrollment resolves
      to one run, and to the xPro course wrike_course_codes_of_mitxpro_courses gives the
      code in dim_course. xPro runs with no external id carry '' rather than null, and are
      left out: an enrollment with an empty run code would otherwise fan out across all
      of them.
    #}
    select
        courserun_external_readable_id
        , courserun_id
        , courserun_readable_id
        , courserun_title
    from (
        select
            courserun_external_readable_id
            , courserun_id
            , courserun_readable_id
            , courserun_title
            , row_number() over (
                partition by courserun_external_readable_id
                order by courserun_created_on, courserun_id
            ) as _row_num
        from {{ ref('int__mitxpro__course_runs') }}
        where courserun_external_readable_id is not null
            and courserun_external_readable_id != ''
    ) as ranked
    where _row_num = 1
{%- endmacro %}
