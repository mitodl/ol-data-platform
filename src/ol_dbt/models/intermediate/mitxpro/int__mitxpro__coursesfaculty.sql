with
    coursepages as (select * from {{ ref("stg__mitxpro__app__postgres__cms_coursepage") }}),
    facultymemberspage as (
        select
            *, cast(json_parse(cms_facultymemberspage_faculty) as array(json)) as cms_facultymemberspage_faculty_array
        from {{ ref("stg__mitxpro__app__postgres__cms_facultymemberspage") }}
    ),
    wagtailpages as (select * from {{ ref("stg__mitxpro__app__postgres__wagtail_page") }}),
    unnestedfacultymemberspage as (
        select
            facultymemberspage.wagtail_page_id,
            json_format(cms_facultymemberspage_facultymember) as cms_facultymemberspage_facultymember  -- noqa

        from facultymemberspage
        cross join
            unnest(facultymemberspage.cms_facultymemberspage_faculty_array) as t(cms_facultymemberspage_facultymember)  -- noqa
    ),
    coursepageswithpath as (
        select coursepages.wagtail_page_id, coursepages.course_id, wagtailpages.wagtail_page_path
        from coursepages
        inner join wagtailpages on coursepages.wagtail_page_id = wagtailpages.wagtail_page_id
    )

select
    coursepageswithpath.course_id,
    json_query(
        unnestedfacultymemberspage.cms_facultymemberspage_facultymember, 'lax $.value.name'
    ) as cms_facultymemberspage_facultymember_name,
    json_query(
        unnestedfacultymemberspage.cms_facultymemberspage_facultymember, 'lax $.value.description'
    ) as cms_facultymemberspage_facultymember_description
from unnestedfacultymemberspage
inner join wagtailpages on unnestedfacultymemberspage.wagtail_page_id = wagtailpages.wagtail_page_id
inner join coursepageswithpath on wagtailpages.wagtail_page_path like coursepageswithpath.wagtail_page_path || '%'
