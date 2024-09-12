with instructors as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_instructorpage') }}
)

, instructor_pagelinks as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_instructorpagelink') }}
)

, course_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
)

select distinct
    course_pages.course_id
    , instructors.instructor_name
    , instructors.instructor_title
    , instructors.instructor_bio_short
    , instructors.instructor_bio_long
from course_pages
inner join instructor_pagelinks on course_pages.wagtail_page_id = instructor_pagelinks.wagtail_page_id
inner join instructors on instructor_pagelinks.instructor_wagtail_page_id = instructors.wagtail_page_id
