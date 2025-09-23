with
    af as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_assessmentfeedback") }}
    ),
    afa as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_assessmentfeedback_assessments"
                )
            }}
    ),
    a as (select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_assessment") }}),
    s as (select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_submission") }}),
    si as (select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__submissions_studentitem") }})

select distinct af.id, af.submission_uuid, af.feedback_text
from af
inner join afa on af.id = afa.assessmentfeedback_id
inner join a on afa.assessment_id = a.id
inner join s on a.submission_uuid = s.uuid
inner join si on s.student_item_id = si.id
