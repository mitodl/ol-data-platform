with exam_grades as (
    select * from {{ ref('int__micromasters__exam_grades') }}
)

select *
from exam_grades
