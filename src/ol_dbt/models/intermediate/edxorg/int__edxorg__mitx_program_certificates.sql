with completed_program_learners as (
    select * from {{ ref('stg__edxorg__s3__program_learner_report') }}
    where user_has_completed_program = true
)

, completed_program_learners_sorted as (
    select
        *
        , row_number() over (partition by user_id, program_uuid order by program_certificate_awarded_on desc) as row_num
    from completed_program_learners
)

select
    program_type
    , program_uuid
    , program_title
    , user_id
    , user_username
    , user_full_name
    , user_has_completed_program
    , program_certificate_awarded_on
from completed_program_learners_sorted
where row_num = 1
