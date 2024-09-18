with topics as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_coursetopic') }}
)

, course_topics as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage_topics') }}
)

, course_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
)

select
    course_pages.course_id
    , course_topics.coursetopic_id
    , topics.coursetopic_parent_id
    , topics.coursetopic_name
from course_pages
inner join course_topics
    on course_pages.wagtail_page_id = course_topics.wagtail_page_id
inner join topics
    on course_topics.coursetopic_id = topics.coursetopic_id
