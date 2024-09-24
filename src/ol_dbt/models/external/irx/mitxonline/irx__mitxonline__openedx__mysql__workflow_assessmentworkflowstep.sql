select
    workflow_id
    , skipped
    , order_num
    , assessment_completed_at
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__workflow_assessmentworkflowstep') }}
where workflow_id in (
    select assessmentworkflow.id
    from
        {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__workflow_assessmentworkflow') }}
        as assessmentworkflow
)
