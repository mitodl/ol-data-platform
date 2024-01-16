with course_certificates as (
    select * from {{ ref('int__mitx__courserun_certificates') }}
    where platform = '{{ var("mitxonline") }}'
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    course_certificates.course_number
    , course_certificates.courserun_title
    , course_certificates.courserun_readable_id
    , course_certificates.courseruncertificate_url
    , course_certificates.courseruncertificate_created_on
    , course_certificates.user_mitxonline_username as user_username
    , course_certificates.user_email
    , course_certificates.user_full_name
    , users.openedx_user_id
from course_certificates
left join users
    on (
        course_certificates.user_mitxonline_username = users.user_username
        or course_certificates.user_email = users.user_email
    )
