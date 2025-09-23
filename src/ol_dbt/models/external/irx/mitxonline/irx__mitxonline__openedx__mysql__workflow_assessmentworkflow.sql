with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__workflow_assessmentworkflow") }}
    )

    {{ deduplicate_raw_table(order_by="modified", partition_columns="id") }}
select course_id, status, modified, id
from most_recent_source
