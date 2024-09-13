-- Course information for MITx Online

with courses as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
)

, course_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_wagtail_page') }}
)

select
    courses.course_id
    , courses.course_title
    , courses.course_is_live
    , courses.course_readable_id
    , courses.course_number
    , course_pages.course_description
    , course_pages.course_length
    , course_pages.course_effort
    , course_pages.course_prerequisites
    , course_pages.course_about
    , course_pages.course_what_you_learn
    , wagtail_page.wagtail_page_slug as course_page_slug
    , wagtail_page.wagtail_page_url_path as course_page_url_path
    , wagtail_page.wagtail_page_is_live as course_page_is_live
    , wagtail_page.wagtail_page_first_published_on as course_page_first_published_on
    , wagtail_page.wagtail_page_last_published_on as course_page_last_published_on
from courses
left join course_pages on courses.course_id = course_pages.course_id
left join wagtail_page on course_pages.wagtail_page_id = wagtail_page.wagtail_page_id
