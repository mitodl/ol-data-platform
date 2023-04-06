-- Course Runs information for Bootcamps

with runs as (
    select *
    from dev.main_staging.stg__bootcamps__app__postgres__courses_courserun
)

select
    courserun_id
    , course_id
    , courserun_title
    , courserun_readable_id
    , courserun_start_on
    , courserun_end_on
from runs
