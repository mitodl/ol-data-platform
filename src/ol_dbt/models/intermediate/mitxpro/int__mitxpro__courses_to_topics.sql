with coursepagetopic as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage_topics') }}
)

, coursepages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
)

select
    coursepages.course_id
    , coursepagetopic.coursetopic_id
from coursepagetopic
inner join coursepages
    on coursepages.wagtail_page_id = coursepagetopic.wagtail_page_id
