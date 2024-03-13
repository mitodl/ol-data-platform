with enrollments as (
    select *
    from {{ ref('int__micromasters__course_enrollments') }}
)

, course_certificates as (
    select *
    from {{ ref('int__micromasters__course_certificates') }}
)

, program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

, enrollments_by_program as (
    select
        micromasters_program_id
        , mitxonline_program_id
        , arbitrary(program_title) as program_title
        , count(*) as total_enrollments
        , count(distinct user_email) as unique_users
        , count(distinct user_address_country) as unique_countries
        , count_if(courserunenrollment_enrollment_mode = 'verified') as verified_enrollments
        , count(distinct case when courserunenrollment_enrollment_mode = 'verified' then user_email end)
        as unique_verified_users
    from enrollments
    group by micromasters_program_id, mitxonline_program_id
)

, enrollments_total as (
    select
        'total' as program_title
        , 0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(distinct concat_ws(',', cast(user_id as varchar), platform, courserun_readable_id)) as total_enrollments
        , count(distinct user_email) as unique_users
        , count(distinct user_address_country) as unique_countries
        , count_if(courserunenrollment_enrollment_mode = 'verified') as verified_enrollments
        , count(distinct case when courserunenrollment_enrollment_mode = 'verified' then user_email end)
        as unique_verified_users
    from enrollments
)

, enrollments_combined as (
    select
        program_title
        , micromasters_program_id
        , mitxonline_program_id
        , total_enrollments
        , unique_users
        , unique_countries
        , verified_enrollments
        , unique_verified_users
    from enrollments_by_program
    union all
    select
        program_title
        , micromasters_program_id
        , mitxonline_program_id
        , total_enrollments
        , unique_users
        , unique_countries
        , verified_enrollments
        , unique_verified_users
    from enrollments_total
)

, course_certificates_by_program as (
    select
        micromasters_program_id
        , mitxonline_program_id
        , count(*) as course_certificates
        , count(distinct user_email) as unique_course_certificate_earners
    from course_certificates
    group by micromasters_program_id, mitxonline_program_id
)

, course_certificates_total as (
    select
        0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(distinct concat_ws(',', courserun_readable_id, user_edxorg_username, user_mitxonline_username))
        as course_certificates
        , count(distinct user_email) as unique_course_certificate_earners
    from course_certificates
)

, course_certificates_combined as (
    select
        micromasters_program_id
        , mitxonline_program_id
        , course_certificates
        , unique_course_certificate_earners
    from course_certificates_by_program
    union all
    select
        micromasters_program_id
        , mitxonline_program_id
        , course_certificates
        , unique_course_certificate_earners
    from course_certificates_total
)

, program_certificates_by_program as (
    select
        micromasters_program_id
        , mitxonline_program_id
        , count(*) as program_certificates
    from program_certificates
    group by micromasters_program_id, mitxonline_program_id
)

, program_certificates_total as (
    select
        0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(*) as program_certificates
    from program_certificates
)

, program_certificates_combined as (
    select
        micromasters_program_id
        , mitxonline_program_id
        , program_certificates
    from program_certificates_by_program
    union all
    select
        micromasters_program_id
        , mitxonline_program_id
        , program_certificates
    from program_certificates_total
)

select
    enrollments_combined.program_title
    , enrollments_combined.total_enrollments
    , enrollments_combined.unique_users
    , enrollments_combined.unique_countries
    , enrollments_combined.verified_enrollments
    , enrollments_combined.unique_verified_users
    , course_certificates_combined.course_certificates
    , course_certificates_combined.unique_course_certificate_earners
    , program_certificates_combined.program_certificates
from enrollments_combined
left join course_certificates_combined
    on
        enrollments_combined.micromasters_program_id = course_certificates_combined.micromasters_program_id
        or enrollments_combined.mitxonline_program_id = course_certificates_combined.mitxonline_program_id
left join program_certificates_combined
    on
        enrollments_combined.micromasters_program_id = program_certificates_combined.micromasters_program_id
        or enrollments_combined.mitxonline_program_id = program_certificates_combined.mitxonline_program_id
order by enrollments_combined.micromasters_program_id, enrollments_combined.mitxonline_program_id
