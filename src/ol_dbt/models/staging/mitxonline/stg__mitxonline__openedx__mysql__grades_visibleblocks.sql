with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__grades_visibleblocks") }}
    ),
    cleaned as (

        select
            id as visibleblocks_id,
            course_id as courserun_readable_id,
            hashed as visibleblocks_hash,
            blocks_json as visibleblocks_json
        from source
    )

select *
from cleaned
