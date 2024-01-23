select
    workflow_id
    , order_num
    , started_at
    , id
from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_studenttrainingworkflowitem') }}
