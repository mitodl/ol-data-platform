select
    scheduled_at
    , submission_uuid
    , algorithm_id
from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aigradingworkflow') }}
