with courseruns as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

select
    courserun_readable_id
    , split_part(replace(replace(courserun_readable_id, 'course-v1:', ''), 'UAI_', ''), '+', 1) as organization
from courseruns
where
    courserun_readable_id like '%UAI%'
    and courserun_readable_id not like '%UAI_SOURCE%'
