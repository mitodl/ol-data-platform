with programpages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_programpage') }}
)

, facultymemberspage as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__cms_facultymemberspage') }}
)


, wagtailpages as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__wagtail_page') }}
)


, unnestedfacultymemberspage as (
    select
        facultymemberspage.wagtail_page_id
        , json_format(cms_facultymemberspage_facultymember) as cms_facultymemberspage_facultymember -- noqa

    from facultymemberspage
    cross join
        unnest(cast(json_parse(cms_facultymemberspage_faculty) as array (json)))
        as t(cms_facultymemberspage_facultymember) -- noqa

)

, programspageswithpath as (
    select
        programpages.wagtail_page_id
        , programpages.program_id
        , wagtailpages.wagtail_page_path
    from programpages
    inner join wagtailpages
        on programpages.wagtail_page_id = wagtailpages.wagtail_page_id
)

select
    programspageswithpath.program_id
    , json_query(unnestedfacultymemberspage.cms_facultymemberspage_facultymember, 'lax $.value.name')
    as cms_facultymemberspage_facultymember_name
    , json_query(unnestedfacultymemberspage.cms_facultymemberspage_facultymember, 'lax $.value.description')
    as cms_facultymemberspage_facultymember_description


from unnestedfacultymemberspage
inner join wagtailpages
    on unnestedfacultymemberspage.wagtail_page_id = wagtailpages.wagtail_page_id
inner join programspageswithpath
    on wagtailpages.wagtail_page_path like programspageswithpath.wagtail_page_path || '%'
