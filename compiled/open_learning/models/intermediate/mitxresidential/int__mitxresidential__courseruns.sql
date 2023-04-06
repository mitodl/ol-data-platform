with runs as (
    select * from dev.main_staging.stg__mitxresidential__openedx__courserun
)

select
    courserun_readable_id
    , courserun_title
    , courserun_org
    , courserun_course_number
    , courserun_start_on
    , courserun_end_on
    , courserun_enrollment_start_on
    , courserun_enrollment_end_on
    , courserun_is_self_paced
    , courserun_created_on
from runs
