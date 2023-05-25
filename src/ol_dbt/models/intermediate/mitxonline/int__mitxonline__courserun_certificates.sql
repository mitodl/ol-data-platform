-- Course Certificate information for MITx Online

with certificates as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courseruncertificate') }}
)

, runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, courserun_certificates as (
    select
        certificates.courseruncertificate_id
        , certificates.courseruncertificate_uuid
        , certificates.courserun_id
        , runs.courserun_title
        , runs.course_number
        , runs.courserun_readable_id
        , runs.courserun_url
        , runs.courserun_platform
        , runs.course_id
        , certificates.courseruncertificate_url
        , certificates.courseruncertificate_is_revoked
        , certificates.courseruncertificate_created_on
        , certificates.courseruncertificate_updated_on
        , certificates.user_id
        , users.user_username
        , users.user_edxorg_username
        , users.user_email
        , users.user_full_name
    from certificates
    inner join runs on certificates.courserun_id = runs.courserun_id
    inner join users on certificates.user_id = users.user_id
)

select * from courserun_certificates
