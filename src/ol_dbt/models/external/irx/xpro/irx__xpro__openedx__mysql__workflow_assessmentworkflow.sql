select
    course_id
    , status
    , modified
    , id
from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__workflow_assessmentworkflow') }}
