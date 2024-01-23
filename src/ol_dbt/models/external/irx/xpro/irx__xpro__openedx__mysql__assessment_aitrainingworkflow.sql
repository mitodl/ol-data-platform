select
    course_id
    , id
    , item_idm
        as uuid
from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_aitrainingworkflow') }}
