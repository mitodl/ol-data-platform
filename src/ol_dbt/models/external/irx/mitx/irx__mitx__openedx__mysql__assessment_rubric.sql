with rub as (
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

select distinct rub.id, rub.content_hash, rub.structure_hash from rub
inner join a on rub.id = a.rubric_id
inner join s on a.submission_uuid = s.uuid
inner join si on s.student_item_id = si.id
