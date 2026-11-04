-- In dev, empty until the student_risk_probability asset has materialized in this schema.
{% set risk = dev_schema_source('reporting', 'student_risk_probability') %}

with cheating_detection as (
    select * from {{ ref('cheating_detection_report') }}
)

, student_risk_probability as (
{% if not risk.is_unit_test and execute and not risk.resolved_relation %}
    select
        cast(null as varchar) as courserun_readable_id
        , cast(null as bigint) as openedx_user_id
        , cast(null as double) as risk_probability
        , cast(null as timestamp) as created_timestamp
    where false
{% else %}
    select * from {{ risk.relation_ref }}
{% endif %}
)

, student_grades as (
    select
        platform,
        openedx_user_id,
        max(email) as email,
        courserun_readable_id,
        max(courserungrade_grade) AS courserungrade_grade
    from cheating_detection
    group by platform, openedx_user_id, courserun_readable_id
)

select
    student_grades.platform,
    student_grades.openedx_user_id,
    student_grades.email,
    student_grades.courserun_readable_id,
    student_grades.courserungrade_grade,
    student_risk_probability.risk_probability,
    student_risk_probability.created_timestamp
from student_grades
left join student_risk_probability
    on student_grades.openedx_user_id = student_risk_probability.openedx_user_id
    and student_grades.courserun_readable_id = student_risk_probability.courserun_readable_id
