-- Course Certificate information for Bootcamps

with certificates as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courseruncertificate') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, bootcamps_certificates as (
    select
        certificates.courseruncertificate_id
        , certificates.courseruncertificate_uuid
        , certificates.courserun_id
        , runs.courserun_title
        , runs.courserun_readable_id
        , runs.course_id
        , certificates.courseruncertificate_is_revoked
        , certificates.courseruncertificate_url
        , certificates.courseruncertificate_created_on
        , certificates.courseruncertificate_updated_on
        , certificates.user_id
        , users.user_username
        , users.user_email
        , users.user_full_name
    from certificates
    inner join runs on certificates.courserun_id = runs.courserun_id
    inner join users on certificates.user_id = users.user_id
)

select * from bootcamps_certificates
