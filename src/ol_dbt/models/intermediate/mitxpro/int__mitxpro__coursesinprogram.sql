with coursesinprogram as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursesinprogrampage') }}
)

, coursepages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
)

, programpages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_programpage') }}
)


, wagtailpages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__wagtail_page') }}
)

, unnestedcoursesinprogram as (
    select

        coursesinprogram.wagtail_page_id as coursesinprogrampage_wagtail_page_id
        , coursepage_wagtail_page_id --noqa

    from coursesinprogram
    cross join
        unnest(coursesinprogram.cms_coursesinprogrampage_coursepage_wagtail_page_ids)
        as t(coursepage_wagtail_page_id) --noqa
)

, programpageswithpath as (
    select
        programpages.wagtail_page_id
        , programpages.program_id
        , wagtailpages.wagtail_page_path
    from programpages
    inner join wagtailpages
        on programpages.wagtail_page_id = wagtailpages.wagtail_page_id
)

select
    coursepages.course_id
    , programpageswithpath.program_id
from unnestedcoursesinprogram
left join coursepages
    on unnestedcoursesinprogram.coursepage_wagtail_page_id = coursepages.wagtail_page_id
left join wagtailpages
    on unnestedcoursesinprogram.coursesinprogrampage_wagtail_page_id = wagtailpages.wagtail_page_id
inner join programpageswithpath
    on wagtailpages.wagtail_page_path like programpageswithpath.wagtail_page_path || '%'
