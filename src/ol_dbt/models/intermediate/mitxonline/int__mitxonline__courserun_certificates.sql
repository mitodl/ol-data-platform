-- Course Certificate information for MITx Online

with certificates as (
    select *
    from
        {{ ref('stg__mitxonline__app__postgres__courses_courseruncertificate') }}
)

, runs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

select
    certificates.courseruncertificate_id
    , certificates.courseruncertificate_uuid
    , certificates.courserun_id
    , runs.courserun_title
    , runs.courserun_readable_id
    , runs.courserun_url
    , runs.course_id
    , certificates.courseruncertificate_is_revoked
    , certificates.user_id
    , users.user_username
    , users.user_email
from certificates
inner join runs on certificates.courserun_id = runs.courserun_id
inner join users on certificates.user_id = users.user_id
