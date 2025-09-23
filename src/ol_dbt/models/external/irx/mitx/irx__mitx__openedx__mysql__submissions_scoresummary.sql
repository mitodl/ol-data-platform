select highest_id, id, latest_id, student_item_id
from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_scoresummary") }}
where
    student_item_id in (
        select studentitem.id
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_studentitem") }} as studentitem
    )
