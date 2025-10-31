with edx_certificate as (
    select
        *
        ,  {{ format_course_id('courserun_readable_id', false) }} as courseware_id
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, edx_run as (
    select * from {{ ref('edxorg_to_mitxonline_course_runs') }}
)

, mitx_user as (
    select * from {{ ref('int__mitx__users') }}
)

select distinct
    edx_certificate.user_email
    , mitx_user.user_full_name
    , mitx_user.user_gender
    , mitx_user.user_birth_year
    , mitx_user.user_address_country
from edx_certificate
inner join edx_run on edx_certificate.courseware_id = edx_run.courseware_id ---match only the migrated course runs
left join mitx_user on edx_certificate.user_mitxonline_username = mitx_user.user_mitxonline_username
left join mitx_user as mitx_user2 on edx_certificate.user_email = mitx_user2.user_mitxonline_email
where
    mitx_user.user_mitxonline_username is null
    and mitx_user2.user_mitxonline_email is null
