-- Course information for Bootcamps
with courses as (select * from {{ ref("stg__bootcamps__app__postgres__courses_course") }})

select course_id, course_title, course_readable_id
from courses
