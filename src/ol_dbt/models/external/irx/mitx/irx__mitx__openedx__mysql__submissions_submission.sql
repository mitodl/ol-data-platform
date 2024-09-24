select
    team_submission_id
    , submitted_at
    , status
    , id
from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__submissions_submission') }}
where student_item_id in (
    select studentitem.id
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__submissions_studentitem') }} as studentitem
)
