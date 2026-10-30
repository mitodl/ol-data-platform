{#
  One row per (platform, courserun_readable_id) with the title of the course run's
  current course. dim_course_run and dim_course are SCD2 and can hold more than one
  is_current = true row per key, so the latest wins. Runs with no dim_course row (all
  residential runs) use the run title.
#}
{% macro current_course_titles() %}
select
    courserun_readable_id
    , platform
    , course_title
from (
    select
        dim_course_run.courserun_readable_id
        , dim_course_run.platform
        , coalesce(dim_course.course_title, dim_course_run.courserun_title) as course_title
        , row_number() over (
            partition by dim_course_run.platform, dim_course_run.courserun_readable_id
            order by
                dim_course_run.effective_date desc nulls last
                , dim_course.effective_date desc nulls last
        ) as _row_num
    from {{ ref('dim_course_run') }} as dim_course_run
    left join {{ ref('dim_course') }} as dim_course
        on
            dim_course_run.course_fk = dim_course.course_pk
            and dim_course.is_current = true
    where dim_course_run.is_current = true
)
where _row_num = 1
{% endmacro %}
