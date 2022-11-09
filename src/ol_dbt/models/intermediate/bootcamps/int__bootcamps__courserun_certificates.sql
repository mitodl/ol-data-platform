-- Course Certificate information for Bootcamps

with certificates as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courseruncertificate') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

select
    certificates.courseruncertificate_id
    , certificates.courseruncertificate_uuid
    , certificates.courserun_id
    , runs.courserun_title
    , runs.courserun_readable_id
    , runs.course_id
    , certificates.courseruncertificate_is_revoked
    , certificates.user_id
    , users.user_username
    , users.user_email
from certificates
inner join runs on certificates.courserun_id = runs.courserun_id
inner join users on certificates.user_id = users.user_id
