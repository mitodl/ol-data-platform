with program_learners as (
    select *
    from {{ ref('stg__edxorg__s3__program_learner_report') }}
)

, micromasters_programs as (
    select * from {{ ref('int__mitx__programs') }}
    where is_micromasters_program = true
)

select
    program_learners.program_type
    , program_learners.program_uuid
    , program_learners.program_title
    , program_learners.user_id
    , program_learners.user_username
    , program_learners.user_full_name
    , program_learners.user_has_completed_program
    , micromasters_programs.micromasters_program_id
from program_learners
left join micromasters_programs
    on (
        program_learners.program_title like micromasters_programs.program_title || '%'
        or program_learners.program_title like '%' || micromasters_programs.program_title
    )
group by
    program_learners.program_type
    , program_learners.program_uuid
    , program_learners.program_title
    , program_learners.user_id
    , program_learners.user_username
    , program_learners.user_full_name
    , program_learners.user_has_completed_program
    , micromasters_programs.micromasters_program_id
