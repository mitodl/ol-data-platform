-- Program information for MITx Online

with programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, program_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_programpage') }}
)

, program_requirements as (
    select
        program_id
        , course_id
    from {{ ref('int__mitxonline__program_requirements') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_wagtail_page') }}
)

, course_topics as (
    select * from {{ ref('int__mitxonline__course_to_topics') }}
)

, program_topics as (
    select
        program_requirements.program_id
        , array_join(array_distinct(array_agg(course_topics.coursetopic_name)), ', ') as program_topics
    from course_topics
    inner join program_requirements on course_topics.course_id = program_requirements.course_id
    group by program_requirements.program_id
)

, program_instructors as (
    select
        program_id
        , array_join(array_agg(instructor_name), ', ') as program_instructors
    from {{ ref('int__mitxonline__program_instructors') }}
    group by program_id
)

select
    programs.program_id
    , programs.program_title
    , programs.program_is_live
    , programs.program_readable_id
    , programs.program_type
    , programs.program_availability
    , programs.program_certification_type
    , programs.program_is_dedp
    , programs.program_is_micromasters
    , program_pages.program_description
    , program_pages.program_price
    , program_pages.program_length
    , program_pages.program_effort
    , program_pages.program_prerequisites
    , program_pages.program_about
    , program_pages.program_what_you_learn
    , program_topics.program_topics
    , program_instructors.program_instructors
    , wagtail_page.wagtail_page_slug as program_page_slug
    , wagtail_page.wagtail_page_url_path as program_page_url_path
    , wagtail_page.wagtail_page_is_live as program_page_is_live
    , wagtail_page.wagtail_page_first_published_on as program_page_first_published_on
    , wagtail_page.wagtail_page_last_published_on as program_page_last_published_on
from programs
left join program_pages on programs.program_id = program_pages.program_id
left join wagtail_page on program_pages.wagtail_page_id = wagtail_page.wagtail_page_id
left join program_topics on programs.program_id = program_topics.program_id
left join program_instructors on programs.program_id = program_instructors.program_id
