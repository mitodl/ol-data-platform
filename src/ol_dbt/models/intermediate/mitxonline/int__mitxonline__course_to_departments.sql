-- MITx Online Course to Department Information
-- Keep it as separate model for flexibility to satisfy different use cases

with departments as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_department') }}
)

, course_to_departments as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_course_to_department') }}
)

select department.coursedepartment_name
from course_to_departments
inner join department on course_to_departments.coursedepartment_id = departments.coursedepartment_id
