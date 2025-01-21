with aafo as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_assessmentfeedbackoption') }}
)

, afo as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_assessmentfeedback_options') }}
)

, af as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_assessmentfeedback') }}
)

, afa as (
    select *
    from
        {{
            source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__assessment_assessmentfeedback_assessments')
        }}
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
    aafo.text
    , aafo.id
from aafo
inner join afo on aafo.id = afo.assessmentfeedbackoption_id
inner join af on afo.assessmentfeedback_id = af.id
inner join afa on af.id = afa.assessmentfeedback_id
inner join a on afa.assessment_id = a.id
inner join s on a.submission_uuid = s.uuid
inner join si on s.student_item_id = si.id
