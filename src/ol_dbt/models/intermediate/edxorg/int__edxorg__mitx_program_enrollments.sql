with
    program_learners as (select * from {{ ref("stg__edxorg__s3__program_learner_report") }}),
    micromasters_programs as (select * from {{ ref("int__mitx__programs") }} where is_micromasters_program = true),
    program_learners_sorted as (
        select
            *,
            row_number() over (
                partition by user_id, program_uuid
                order by program_certificate_awarded_on desc, courserunenrollment_created_on desc
            ) as row_num
        from program_learners
    )

select
    program_learners_sorted.program_type,
    program_learners_sorted.program_uuid,
    program_learners_sorted.program_title,
    program_learners_sorted.user_id,
    program_learners_sorted.user_username,
    program_learners_sorted.user_full_name,
    program_learners_sorted.user_has_completed_program,
    micromasters_programs.micromasters_program_id,
    program_learners_sorted.program_track,
    coalesce(micromasters_programs.program_title, program_learners_sorted.program_title) as program_name
from program_learners_sorted
left join
    micromasters_programs
    on (
        program_learners_sorted.program_title like micromasters_programs.program_title || '%'
        or program_learners_sorted.program_title like '%' || micromasters_programs.program_title
    )
where program_learners_sorted.row_num = 1
