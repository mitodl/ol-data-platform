-- Course Runs information for MITx Online

with runs as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

select
    courserun_id
    , course_id
    , courserun_title
    , courserun_readable_id
    , courserun_tag
    , courserun_url
    , courserun_start_on
    , courserun_end_on
    , courserun_enrollment_start_on
    , courserun_enrollment_end_on
    , courserun_upgrade_deadline
    , courserun_is_self_paced
    , courserun_is_live
from runs
