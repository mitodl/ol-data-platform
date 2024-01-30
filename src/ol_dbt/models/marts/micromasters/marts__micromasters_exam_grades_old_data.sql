with exam_grades as (
    select * from {{ ref('int__micromasters__exam_grades_old_data') }}
)

select *
from exam_grades
