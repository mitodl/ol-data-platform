select
    student_id
    , course_id
    , id
    , item_id
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__submissions_studentitem') }}
