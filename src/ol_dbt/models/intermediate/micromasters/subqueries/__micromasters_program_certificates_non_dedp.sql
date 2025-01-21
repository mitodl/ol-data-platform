{{ config(materialized='view') }}

with micromasters_program_certificates as (
    --- There are learners who received both 'Statistics and Data Science (General track)' and 'Statistics and Data
    --  Science' from 2U data, but we only count them once in 'Statistics and Data Science' for MM program certificates
    --  report.
    select
        *
        , row_number() over (
            partition by user_id, micromasters_program_id order by program_title
        ) as row_num
    from {{ ref('int__edxorg__mitx_program_certificates') }}
    where program_type = 'MicroMasters'
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, programs as (
    select *
    from {{ ref('int__mitx__programs') }}
    where is_dedp_program = false
)

, program_certificates_override_list as (
    select *
    from {{ ref('stg__micromasters__app__user_program_certificate_override_list') }}
)

, non_dedp_certificates as (
    select
        edx_users.user_username as user_edxorg_username
        , programs.micromasters_program_id
        , micromasters_program_certificates.program_title
        , programs.mitxonline_program_id
        , micromasters_program_certificates.user_id as user_edxorg_id
        , micromasters_program_certificates.program_certificate_hashed_id
        , micromasters_program_certificates.program_certificate_awarded_on as program_completion_timestamp
    from micromasters_program_certificates
    left join edx_users
        on micromasters_program_certificates.user_id = edx_users.user_id
    left join programs
        on micromasters_program_certificates.micromasters_program_id = programs.micromasters_program_id
    where micromasters_program_certificates.row_num = 1
)

-- Some users should recieve a certificate even though they don't fulfill the requirements according
-- to the course certificates in the edxorg database. A list of these users' user ids on edx and the micromasters
-- ids on production for the program they should earn a certificate for are stored in
-- stg__micromasters__app__postgres__courses_course. These ids won't match correctly in qa

, non_dedp_overides as (
    select
        edx_users.user_username as user_edxorg_username
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , edx_users.user_id as user_edxorg_id
        , program_certificates_override_list.program_certificate_hashed_id
        , null as program_completion_timestamp
    from program_certificates_override_list
    inner join edx_users
        on program_certificates_override_list.user_edxorg_id = edx_users.user_id
    inner join programs
        on program_certificates_override_list.micromasters_program_id = programs.micromasters_program_id
    left join non_dedp_certificates
        on
            program_certificates_override_list.user_edxorg_id
            = non_dedp_certificates.user_edxorg_id and program_certificates_override_list.micromasters_program_id
            = non_dedp_certificates.micromasters_program_id
    where non_dedp_certificates.user_edxorg_username is null
)

select *
from non_dedp_certificates

union all

select *
from non_dedp_overides
