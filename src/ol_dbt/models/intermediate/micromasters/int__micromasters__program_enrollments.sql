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

, programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline_dedp_records as (
    select
        '{{ var("mitxonline") }}' as platform_name
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitxonline_programenrollments.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , mitxonline_programenrollments.program_id as mitxonline_program_id
        , mitx_users.user_edxorg_id
        , mitx_users.user_address_city
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , mitx_users.user_full_name
        , mitx_users.user_micromasters_id as micromasters_user_id
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_gender
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
    from mitxonline_programenrollments
    inner join programs
        on mitxonline_programenrollments.program_id = programs.mitxonline_program_id
    inner join mitx_users
        on mitxonline_programenrollments.user_id = mitx_users.user_mitxonline_id
    where programs.is_dedp_program = true
)

, mm_dedp_records as (
    select
        mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_micromasters_email as user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , mitx_users.user_edxorg_id
        , mitx_users.user_gender
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_city
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_full_name
        , mitx_users.user_micromasters_id as micromasters_user_id
        , mitx_users.user_birth_year as user_year_of_birth
        , if(mitx_users.is_mitxonline_user = true, '{{ var("mitxonline") }}', '{{ var("edxorg") }}') as platform_name
    from mm_program_enrollments
    inner join programs
        on mm_program_enrollments.program_id = programs.micromasters_program_id
    inner join mitx_users
        on mm_program_enrollments.user_id = mitx_users.user_micromasters_id
    left join mitxonline_dedp_records
        on
            mitx_users.user_micromasters_email = mitxonline_dedp_records.user_email
            and programs.program_title = mitxonline_dedp_records.program_title
    where
        programs.is_dedp_program = true
        and mitxonline_dedp_records.user_email is null
)

, non_dedp_records as (
    select
        '{{ var("edxorg") }}' as platform_name
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_edxorg_email as user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , mitx_users.user_edxorg_id
        , mitx_users.user_gender
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_city
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_full_name
        , mitx_users.user_micromasters_id as micromasters_user_id
        , mitx_users.user_birth_year as user_year_of_birth
    from micromasters_program_enrollments
    inner join mitx_users
        on micromasters_program_enrollments.user_id = mitx_users.user_edxorg_id
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
