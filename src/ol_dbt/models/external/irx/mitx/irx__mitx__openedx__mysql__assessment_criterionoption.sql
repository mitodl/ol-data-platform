with c as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_criterion') }}
)

, co as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_criterionoption') }}
)

, rub as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_rubric') }}
)

, a as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_assessment') }}
)

, s as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__submissions_submission') }}
)

, si as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__submissions_studentitem') }}
)

select distinct
    co.order_num
    , co.id
    , co.points
from co
where co.criterion_id in (
    select c.id from c
    where c.rubric_id in (
        select distinct rub.id from rub
        inner join a on rub.id = a.rubric_id
        inner join s on a.submission_uuid = s.uuid
        inner join si on s.student_item_id = si.id
    )
)
