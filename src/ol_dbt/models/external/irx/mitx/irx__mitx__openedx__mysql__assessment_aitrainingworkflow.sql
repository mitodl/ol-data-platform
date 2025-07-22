select
    course_id
    , id
    , item_id
        as uuid
from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aitrainingworkflow') }}
