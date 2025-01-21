with instructors as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_instructorpage') }}
)

, instructor_pagelinks as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_instructorpagelink') }}
)

, program_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_programpage') }}
)

select
    program_pages.program_id
    , instructors.instructor_name
    , instructors.instructor_title
    , instructors.instructor_bio_short
    , instructors.instructor_bio_long
from program_pages
inner join instructor_pagelinks on program_pages.wagtail_page_id = instructor_pagelinks.wagtail_page_id
inner join instructors on instructor_pagelinks.instructor_wagtail_page_id = instructors.wagtail_page_id
