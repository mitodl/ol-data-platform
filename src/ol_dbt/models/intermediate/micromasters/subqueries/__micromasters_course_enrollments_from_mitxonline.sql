{{ config(materialized='view') }}

with mitxonline_enrollments as (
    select *
    from {{ ref('int__mitxonline__courserunenrollments') }}
    where courserunenrollment_platform = '{{ var("mitxonline") }}'
)

, mitxonline_users as (
    select *
    from {{ ref('int__mitxonline__users') }}
)


, micromasters_courses as (
    select
        course_id
        , program_id
        , course_number
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, micromasters_programs as (
    select *
    from {{ ref( 'int__micromasters__programs') }}
)



select
    micromasters_programs.program_title
    , mitxonline_enrollments.user_id
    , mitxonline_enrollments.user_username
    , mitxonline_users.user_address_country as user_country
    , mitxonline_users.user_email
    , mitxonline_users.user_full_name
    , mitxonline_enrollments.courserunenrollment_enrollment_mode
    , mitxonline_enrollments.courserun_readable_id
    , '{{ var("mitxonline") }}' as platform
    , mitxonline_enrollments.courserunenrollment_created_on
    , mitxonline_enrollments.courserunenrollment_is_active
    , mitxonline_enrollments.courserun_title
    , mitxonline_enrollments.course_number
from mitxonline_enrollments
inner join
    micromasters_courses

    on
        replace(
            replace(replace(mitxonline_enrollments.courserun_readable_id, 'course-v1:', ''), '+', '/'), 'MITxT', 'MITx'
        ) like micromasters_courses.course_readable_id
        || '%'
inner join micromasters_programs on micromasters_programs.program_id = micromasters_courses.program_id
inner join mitxonline_users on mitxonline_users.user_id = mitxonline_enrollments.user_id
