with aafo as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessmentfeedbackoption') }}
)

, afo as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessmentfeedback_options') }}
)

, af as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessmentfeedback') }}
)

, afa as (
    select *
    from
        {{
            source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_assessmentfeedback_assessments')
        }}
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

select distinct
    aafo.text
    , aafo.id
from aafo
left join afo on aafo.id = afo.assessmentfeedbackoption_id
left join af on afo.assessmentfeedback_id = af.id
left join afa on af.id = afa.assessmentfeedback_id
left join a on afa.assessment_id = a.id
left join s on a.submission_uuid = s.uuid
left join si on s.student_item_id = si.id
