with co as (
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

, te as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_trainingexample') }}
)

, ate as (
    select *
    from
        {{ source(
            'ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aitrainingworkflow_training_examples'
        ) }}
)

, tw as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aitrainingworkflow') }}
)

, aigw as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aigradingworkflow') }}
)

, acs as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_aiclassifierset') }}
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
        left join a on rub.id = a.rubric_id
        left join s on a.submission_uuid = s.uuid
        left join si on s.student_item_id = si.id
        union distinct
        select distinct rub.id from rub
        left join te on rub.id = te.rubric_id
        left join ate on te.id = ate.trainingexample_id
        left join tw on ate.aitrainingworkflow_id = tw.id
        union distinct
        select distinct rub.id from rub
        left join aigw on rub.id = aigw.rubric_id
        union distinct
        select distinct rub.id from rub
        left join acs on rub.id = acs.rubric_id
    )
)
