-- Course Certificate information for edx.org

with certificates as (
    select
        user_id
        , courserun_readable_id
        , courseruncertificate_is_earned
        , courseruncertificate_status
        , courseruncertificate_created_on
        , courserunenrollment_is_active
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where courseruncertificate_is_earned = true
)

, runs as (
    select
        courserun_readable_id
        , courserun_title
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
)

---- placeholder for users

select
    certificates.user_id
    , certificates.courserun_readable_id
    , certificates.courseruncertificate_status
    , certificates.courseruncertificate_created_on
    , runs.courserun_title
from certificates
---- there are certificates issued for courses that don't exist in course model.
--   this inner joins will eliminate those rows.
--   if we want to show all the certificate, it needs to change to Left join
inner join runs on certificates.courserun_readable_id = runs.courserun_readable_id
