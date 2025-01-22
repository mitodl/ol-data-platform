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

, program_requirements as (
    select * from {{ ref('int__mitxonline__program_requirements') }}
)

, programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, course_certification_type as (
    select * from (
        select
            program_requirements.course_id
            , program_requirements.program_id
            , programs.program_certification_type
            , row_number() over (
                partition by program_requirements.course_id
                order by programs.program_is_micromasters
            ) as row_num
        from program_requirements
        inner join programs on program_requirements.program_id = programs.program_id
    )
    where row_num = 1
)

, course_topics as (
    select
        course_id
        , array_join(array_agg(coursetopic_name), ', ') as course_topics
    from {{ ref('int__mitxonline__course_to_topics') }}
    group by course_id
)

, course_instructors as (
    select
        course_id
        , array_join(array_agg(instructor_name), ', ') as course_instructors
    from {{ ref('int__mitxonline__course_instructors') }}
    group by course_id
)

select
    courses.course_id
    , courses.course_title
    , courses.course_is_live
    , courses.course_readable_id
    , courses.course_number
    , course_pages.course_description
    , course_pages.course_price
    , course_pages.course_length
    , course_pages.course_effort
    , course_pages.course_prerequisites
    , course_pages.course_about
    , course_pages.course_what_you_learn
    , course_topics.course_topics
    , course_instructors.course_instructors
    , wagtail_page.wagtail_page_slug as course_page_slug
    , wagtail_page.wagtail_page_url_path as course_page_url_path
    , wagtail_page.wagtail_page_is_live as course_page_is_live
    , wagtail_page.wagtail_page_first_published_on as course_page_first_published_on
    , wagtail_page.wagtail_page_last_published_on as course_page_last_published_on
    , coalesce(
        course_certification_type.program_certification_type, 'Certificate of Completion'
    ) as course_certification_type
from courses
left join course_pages on courses.course_id = course_pages.course_id
left join wagtail_page on course_pages.wagtail_page_id = wagtail_page.wagtail_page_id
left join course_certification_type on courses.course_id = course_certification_type.course_id
left join course_topics on courses.course_id = course_topics.course_id
left join course_instructors on courses.course_id = course_instructors.course_id
