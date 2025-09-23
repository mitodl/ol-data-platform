select points_earned, reset, submission_id, id, student_item_id
from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_score") }}
where
    student_item_id in (
        select studentitem.id
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_studentitem") }} as studentitem
    )
