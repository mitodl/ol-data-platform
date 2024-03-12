select
    course_id
    , id
    , user_id
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__user_api_usercoursetag') }}
