select
    started_at
    , assessment_id
    , author_id
    , id
from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_peerworkflowitem') }}
