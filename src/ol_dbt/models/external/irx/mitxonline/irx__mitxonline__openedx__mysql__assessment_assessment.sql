with a as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessment') }}
)

, s as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__submissions_submission') }}
)

, si as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__submissions_studentitem') }}
)

select
    a.scorer_id
    , a.id
    , a.score_type
from a
left join s on a.submission_uuid = s.uuid
left join si on s.student_item_id = si.id
