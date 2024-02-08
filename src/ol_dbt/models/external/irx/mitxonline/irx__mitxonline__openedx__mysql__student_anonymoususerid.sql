select
    anonymous_user_id
    , course_id
    , id
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__student_anonymoususerid') }}
