with afa as (
    select *
    from
        {{
            source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_assessmentfeedback_assessments')
        }}
)

, a as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_assessment') }}
)

, s as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__submissions_submission') }}
)

, si as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__submissions_studentitem') }}
)

select
    afa.assessment_id
    , afa.assessmentfeedback_id
    , afa.id
from afa
left join a on afa.assessment_id = a.id
left join s on a.submission_uuid = s.uuid
left join si on s.student_item_id = si.id
