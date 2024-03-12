select
    cancelled_at
    , grading_completed_at
    , student_id
    , course_id
from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_peerworkflow') }}
