with ap as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessmentpart') }}
)

, a as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessment') }}
)

, s as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__submissions_submission') }}
)

, si as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__submissions_studentitem') }}
)

select
    ap.assessment_id
    , ap.option_id
    , ap.id
    , ap.feedback
from ap
left join a on ap.assessment_id = a.id
left join s on a.submission_uuid = s.uuid
left join si on s.student_item_id = si.id
