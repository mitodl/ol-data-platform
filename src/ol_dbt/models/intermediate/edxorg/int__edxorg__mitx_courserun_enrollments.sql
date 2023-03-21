-- Course Run Enrollment information from edx.org

with enrollments as (
    select
        user_id
        , user_username
        , courserun_readable_id
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_is_active
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, runs as (
    select
        courserun_readable_id
        , courserun_title
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
)

, users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, edxorg_enrollments as (
    select
        enrollments.courserun_readable_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_is_active
        , users.user_id
        , users.user_email
        , users.user_username
        , runs.courserun_title
    from enrollments
    inner join
        users on
        enrollments.user_id = users.user_id
    ---- there are certificates issued for courses that don't exist in course model.
    left join runs on enrollments.courserun_readable_id = runs.courserun_readable_id
)

select * from edxorg_enrollments
