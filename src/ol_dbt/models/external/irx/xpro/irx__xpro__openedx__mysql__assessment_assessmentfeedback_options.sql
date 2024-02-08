with afo as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_assessmentfeedback_options') }}
)

, af as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__assessment_assessmentfeedback') }}
)

, afa as (
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

select distinct
    afo.assessmentfeedbackoption_id
    , afo.assessmentfeedback_id
    , afo.id
from afo
left join af on afo.assessmentfeedback_id = af.id
left join afa on af.id = afa.assessmentfeedback_id
left join a on afa.assessment_id = a.id
left join s on a.submission_uuid = s.uuid
left join si on s.student_item_id = si.id
