-- Course Run Enrollment information for edx.org

with enrollments as (
    select
        user_id
        , user_username
        , courserun_readable_id
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_is_active
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
)

, runs as (
    select
        courserun_readable_id
        , courserun_title
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
)

---- placeholder for users

, edxorg_enrollments as (
    select
        enrollments.user_id
        , enrollments.user_username
        , enrollments.courserun_readable_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_is_active
        , runs.courserun_title
    from
        enrollments
    ---- there are certificates issued for courses that don't exist in course model.
    ---- this inner joins will eliminate those rows.
    ---- if we want to show all the certificate, it needs to change to Left join
    inner join
        runs
        on enrollments.courserun_readable_id = runs.courserun_readable_id
)

select * from edxorg_enrollments
