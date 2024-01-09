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

select
    departments.coursedepartment_name
    , course_to_departments.course_id
from course_to_departments
inner join departments on course_to_departments.coursedepartment_id = departments.coursedepartment_id
