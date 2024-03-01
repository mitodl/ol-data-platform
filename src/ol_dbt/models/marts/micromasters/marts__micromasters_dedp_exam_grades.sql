with exam_grades as (
    select * from {{ ref('int__micromasters__dedp_proctored_exam_grades') }}
)

select *
from exam_grades
