with
    completed_program_learners as (
        select *, {{ generate_hash_id("cast(user_id as varchar) || program_uuid") }} as program_certificate_hashed_id
        from {{ ref("stg__edxorg__s3__program_learner_report") }}
        where user_has_completed_program = true
    ),
    micromasters_programs as (select * from {{ ref("int__mitx__programs") }} where is_micromasters_program = true),
    completed_program_learners_sorted as (
        select
            *,
            row_number() over (
                partition by user_id, program_uuid order by program_certificate_awarded_on desc
            ) as row_num
        from completed_program_learners
    )

select
    completed_program_learners_sorted.program_certificate_hashed_id,
    completed_program_learners_sorted.program_type,
    completed_program_learners_sorted.program_uuid,
    completed_program_learners_sorted.program_title,
    completed_program_learners_sorted.user_id,
    completed_program_learners_sorted.user_username,
    completed_program_learners_sorted.user_full_name,
    completed_program_learners_sorted.user_has_completed_program,
    completed_program_learners_sorted.program_certificate_awarded_on,
    micromasters_programs.micromasters_program_id
from completed_program_learners_sorted
left join
    micromasters_programs
    -- - Finance is split into MIT Finance and Finance,
    -- - Statistics and Data Science is split into Statistics and Data Science (General track)
    -- and Statistics and Data Science
    on (
        completed_program_learners_sorted.program_title like micromasters_programs.program_title || '%'
        or completed_program_learners_sorted.program_title like '%' || micromasters_programs.program_title
    )
where completed_program_learners_sorted.row_num = 1
