-- MicroMasters Program Enrollment Information

with micromasters_program_enrollments as (
    --- There are learners who received both 'Statistics and Data Science (General track)' and 'Statistics and Data
    --  Science' from 2U data, but we only count them once in 'Statistics and Data Science' for MM program enrollments
    --  report.
    select
        *
        , row_number() over (
            partition by user_id, micromasters_program_id order by program_title
        ) as row_num
    from {{ ref('int__edxorg__mitx_program_enrollments') }}
    where micromasters_program_id is not null
)

, mm_program_enrollments as (
    select * from {{ ref('stg__micromasters__app__postgres__dashboard_programenrollment') }}
)

, mitxonline_programenrollments as (
    select *
    from {{ ref('int__mitxonline__programenrollments') }}
)

, mitxonline_users as (
    select *
    from {{ ref('int__mitxonline__users') }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)

, programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, mitxonline_dedp_records as (
    select
        micromasters_users.user_edxorg_username
        , mitxonline_programenrollments.user_username as user_mitxonline_username
        , micromasters_users.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , mitxonline_programenrollments.program_id as mitxonline_program_id
        , edx_users.user_id as user_edxorg_id
        , micromasters_users.user_address_city
        , mitxonline_users.user_first_name
        , mitxonline_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , mitxonline_users.user_full_name
        , micromasters_users.user_id as micromasters_user_id
        , 'mitxonline' as platform_name
        , coalesce(
            cast(mitxonline_users.user_birth_year as varchar)
            , substring(micromasters_users.user_birth_date, 1, 4)
        ) as user_year_of_birth
        , coalesce(mitxonline_users.user_gender, micromasters_users.user_gender) as user_gender
        , coalesce(mitxonline_users.user_address_country, micromasters_users.user_address_country) as user_country
        , coalesce(mitxonline_users.user_address_state, micromasters_users.user_address_state_or_territory)
        as user_address_state_or_territory
    from mitxonline_programenrollments
    inner join mitxonline_users
        on mitxonline_programenrollments.user_id = mitxonline_users.user_id
    left join micromasters_users
        on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
    left join edx_users
        on micromasters_users.user_edxorg_username = edx_users.user_username
    inner join programs
        on mitxonline_programenrollments.program_id = programs.mitxonline_program_id
    where programs.is_dedp_program = true
)

, mm_dedp_records as (
    select
        micromasters_users.user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , micromasters_users.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
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
        , micromasters_users.user_id as micromasters_user_id
        , 'micromasters' as platform_name
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from mm_program_enrollments
    inner join micromasters_users
        on mm_program_enrollments.user_id = micromasters_users.user_id
    inner join programs
        on mm_program_enrollments.program_id = programs.micromasters_program_id
    left join edx_users
        on micromasters_users.user_edxorg_username = edx_users.user_username
    left join mitxonline_dedp_records
        on
            micromasters_users.user_email = mitxonline_dedp_records.user_email
            and programs.program_title = mitxonline_dedp_records.program_title
    where
        programs.is_dedp_program = true
        and mitxonline_dedp_records.user_email is null
)

, non_dedp_records as (
    select
        micromasters_program_enrollments.user_username as user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , edx_users.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , micromasters_program_enrollments.user_id as user_edxorg_id
        , edx_users.user_gender
        , edx_users.user_country
        , micromasters_users.user_address_city
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_state_or_territory
        , micromasters_program_enrollments.user_full_name
        , micromasters_users.user_id as micromasters_user_id
        , 'edxorg' as platform_name
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from micromasters_program_enrollments
    inner join edx_users
        on micromasters_program_enrollments.user_id = edx_users.user_id
    left join micromasters_users
        on micromasters_program_enrollments.user_username = micromasters_users.user_edxorg_username
    inner join programs
        on micromasters_program_enrollments.micromasters_program_id = programs.micromasters_program_id
    where
        micromasters_program_enrollments.row_num = 1
        and programs.is_dedp_program = false
)

select
    user_edxorg_username
    , user_mitxonline_username
    , user_email
    , micromasters_program_id
    , program_title
    , mitxonline_program_id
    , user_edxorg_id
    , user_gender
    , user_country
    , user_address_city
    , user_first_name
    , user_last_name
    , user_address_postal_code
    , user_street_address
    , user_address_state_or_territory
    , user_full_name
    , micromasters_user_id
    , user_year_of_birth
    , platform_name
from mitxonline_dedp_records

union distinct

select
    user_edxorg_username
    , user_mitxonline_username
    , user_email
    , micromasters_program_id
    , program_title
    , mitxonline_program_id
    , user_edxorg_id
    , user_gender
    , user_country
    , user_address_city
    , user_first_name
    , user_last_name
    , user_address_postal_code
    , user_street_address
    , user_address_state_or_territory
    , user_full_name
    , micromasters_user_id
    , user_year_of_birth
    , platform_name
from mm_dedp_records

union distinct

select
    user_edxorg_username
    , user_mitxonline_username
    , user_email
    , micromasters_program_id
    , program_title
    , mitxonline_program_id
    , user_edxorg_id
    , user_gender
    , user_country
    , user_address_city
    , user_first_name
    , user_last_name
    , user_address_postal_code
    , user_street_address
    , user_address_state_or_territory
    , user_full_name
    , micromasters_user_id
    , user_year_of_birth
    , platform_name
from non_dedp_records
