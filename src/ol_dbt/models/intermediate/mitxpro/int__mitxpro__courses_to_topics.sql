with coursepagetopic as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage_topics') }}
)

, course_topics as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_coursetopic') }}
)

, coursepages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
)

select
    coursepages.course_id
    , coursepagetopic.coursetopic_id
    , course_topics.coursetopic_parent_coursetopic_id
    , course_topics.coursetopic_name
from coursepagetopic
inner join coursepages
    on coursepagetopic.wagtail_page_id = coursepages.wagtail_page_id
inner join course_topics
    on coursepagetopic.coursetopic_id = course_topics.coursetopic_id
