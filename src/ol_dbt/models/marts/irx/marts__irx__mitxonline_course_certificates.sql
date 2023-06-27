with course_certificates as (
    select * from {{ ref('int__mitx__courserun_certificates') }}
    where platform = '{{ var("mitxonline") }}'
)

select
    course_number
    , courserun_title
    , courserun_readable_id
    , courseruncertificate_url
    , courseruncertificate_created_on
    , user_mitxonline_username as user_username
    , user_email
    , user_full_name
from course_certificates
