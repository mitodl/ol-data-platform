{{ config(materialized='view') }}

with edx_course_certificates as (
    select *
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_program_requirements as (
    select *
    from {{ ref('int__micromasters__program_requirements') }}
    where program_id != {{ var("dedp_micromasters_program_id") }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)


, micromasters_programs as (
    select *
    from {{ ref('int__micromasters__programs') }}
    where program_id != {{ var("dedp_micromasters_program_id") }}
)

, program_certificates_override_list as (
    select *
    from {{ ref('stg__micromasters__app__user_program_certificate_override_list') }}
)

, micromasters_courses as (
    select
        course_id
        , program_id
        , course_edx_key
        , course_number
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, micromasters_user_requirement_completions as (
    select
        edx_course_certificates.user_id as user_edxorg_id
        , edx_course_certificates.user_username as user_edxorg_username
        , edx_course_certificates.courserun_readable_id
        , edx_course_certificates.courseruncertificate_status
        , edx_course_certificates.courseruncertificate_created_on
        , edx_course_certificates.courserun_title
        , micromasters_courses.program_id
        , micromasters_courses.course_id as micromasters_course_id
        , micromasters_program_requirements.programrequirement_type
        , micromasters_program_requirements.program_num_required_courses
        -- The next line makes electiveset_required_number set for both rows for elective completions
        -- and rows for core course completions
        , avg(
            micromasters_program_requirements.electiveset_required_number
        ) over (partition by micromasters_courses.program_id) as electiveset_required_number
        -- We use this for a filter in the next subquery because trino sql does not allow distinct in window
        -- functions and we don't want to count the same course against the program requirements multiple times
        , row_number() over (
            partition by edx_course_certificates.user_id, micromasters_courses.course_id
            order by edx_course_certificates.courseruncertificate_created_on asc
        ) as user_course_certificate_number
    from edx_course_certificates
    inner join
        micromasters_courses
        on
            edx_course_certificates.courserun_readable_id like micromasters_courses.course_readable_id || '%'
    inner join
        micromasters_program_requirements
        on micromasters_program_requirements.course_id = micromasters_courses.course_id
)

-- Some users continue to take courses in the program after earning a program certificate.
-- We calculate a running total of program requirement completions so we can use the date that
-- the user first completes the program requirements as the program completion date.
, micromasters_user_requirement_completions_with_running_total as (
    select
        user_edxorg_id
        , user_edxorg_username
        , program_id
        , program_num_required_courses
        , courseruncertificate_created_on
        , count(
            micromasters_course_id) over (
            partition by user_edxorg_id, program_id
            order by courseruncertificate_created_on asc
        ) as cumulative_courses_completed
        , count(
            case when programrequirement_type = 'Elective' then micromasters_course_id end
        ) over (
            partition by user_edxorg_id, program_id
            order by courseruncertificate_created_on asc
        ) as cumulative_electives_completed
        , coalesce(electiveset_required_number, 0) as electiveset_required_number
    from micromasters_user_requirement_completions
    --we use this filter instead of distinct because Trino sql does not allow distinct in window function
    where user_course_certificate_number = 1

)

, program_completions as (
    select
        user_edxorg_id
        , user_edxorg_username
        , program_id
        -- With the filter, this is the first course after which the user has fulfilled the program requirements
        , min(courseruncertificate_created_on) as program_completion_timestamp
    from micromasters_user_requirement_completions_with_running_total
    where
        cumulative_electives_completed >= electiveset_required_number
        and (
            cumulative_courses_completed - cumulative_electives_completed
            >= program_num_required_courses - electiveset_required_number
        )
    group by user_edxorg_id, user_edxorg_username, program_id
)

, non_dedp_certificates as (
    select
        edx_users.user_username as user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , edx_users.user_email
        , micromasters_programs.program_id as micromasters_program_id
        , micromasters_programs.program_title
        , program_completions.user_edxorg_id
        , edx_users.user_gender
        , edx_users.user_country
        , micromasters_users.user_address_city
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_state_or_territory
        , edx_users.user_full_name
        , program_completions.program_completion_timestamp
        , micromasters_users.user_id as micromasters_user_id
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from program_completions


    left join edx_users
        on edx_users.user_id = program_completions.user_edxorg_id
    left join micromasters_users
        on micromasters_users.user_edxorg_username = program_completions.user_edxorg_username
    left join micromasters_programs
        on micromasters_programs.program_id = program_completions.program_id
)

-- Some users should recieve a certificate even though they don't fulfill the requirements according
-- to the course certificates in the edxorg database. A list of these users' user ids on edx and the micromasters
-- ids on production for the program they should earn a certificate for are stored in
-- stg__micromasters__app__postgres__courses_course. These ids won't match correctly in qa

, non_dedp_overides as (
    select
        edx_users.user_username as user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , edx_users.user_email
        , micromasters_programs.program_id as micromasters_program_id
        , micromasters_programs.program_title
        , edx_users.user_id as user_edxorg_id
        , edx_users.user_gender
        , edx_users.user_country
        , micromasters_users.user_address_city
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_state_or_territory
        , edx_users.user_full_name
        , null as program_completion_timestamp
        , micromasters_users.user_id as micromasters_user_id
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from program_certificates_override_list
    inner join edx_users
        on edx_users.user_id = program_certificates_override_list.user_edxorg_id
    left join micromasters_users
        on micromasters_users.user_edxorg_username = edx_users.user_username
    inner join micromasters_programs
        on micromasters_programs.program_id = program_certificates_override_list.micromasters_program_id
    left join non_dedp_certificates
        on
            non_dedp_certificates.user_edxorg_id
            = program_certificates_override_list.user_edxorg_id and non_dedp_certificates.micromasters_program_id
            = program_certificates_override_list.micromasters_program_id
    where non_dedp_certificates.user_edxorg_username is null
)

select *
from non_dedp_certificates

union all

select *
from non_dedp_overides
