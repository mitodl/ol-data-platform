with workflow_assessmentworkflow as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__workflow_assessmentworkflow') }}
)

{{ deduplicate_query('workflow_assessmentworkflow', 'most_recent_source') }}

select
    course_id
    , status
    , modified
    , id
from most_recent_source
