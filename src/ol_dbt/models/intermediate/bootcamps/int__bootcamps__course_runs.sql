-- Course Runs information for Bootcamps

with runs as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

select
    courserun_id
    , course_id
    , courserun_title
    , courserun_readable_id
    -- Derived from courserun_readable_id by stripping the run suffix (+R{n}), with course_id
    -- appended to ensure uniqueness across courses sharing the same topic/format code
    , split_part(courserun_readable_id, '+', 1) || '+' || split_part(courserun_readable_id, '+', 2)
     || '-' || cast(course_id as varchar) as course_readable_id
    , courserun_start_on
    , courserun_end_on
from runs
