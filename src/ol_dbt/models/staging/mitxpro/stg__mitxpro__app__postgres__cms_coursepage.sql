with coursepage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursepage') }}
)

, externalcoursepage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalcoursepage') }}
)


select
    page_ptr_id as wagtail_page_id
    , description as cms_coursepage_description
    , course_id
    , duration as cms_coursepage_duration
    , format as cms_coursepage_format
    , subhead as cms_coursepage_subhead
    , time_commitment as cms_coursepage_time_commitment
    , catalog_details as cms_coursepage_catalog_details
    , external_marketing_url as cms_coursepage_external_marketing_url
    , 'cms_coursepage' as cms_coursepage_model
from coursepage
union all
select
    externalcoursepage.page_ptr_id as wagtail_page_id
    , externalcoursepage.description as cms_coursepage_description
    , externalcoursepage.course_id
    , externalcoursepage.duration as cms_coursepage_duration
    , externalcoursepage.format as cms_coursepage_format
    , externalcoursepage.subhead as cms_coursepage_subhead
    , externalcoursepage.time_commitment as cms_coursepage_time_commitment
    , externalcoursepage.catalog_details as cms_coursepage_catalog_details
    , externalcoursepage.external_marketing_url as cms_coursepage_external_marketing_url
    , 'cms_externalcoursepage' as cms_coursepage_model

from externalcoursepage
-- There are currently courses that has both a course page and an external course page but
-- adding a dedup in case any are added in the future and to match cms_programpage
left join coursepage
    on externalcoursepage.course_id = coursepage.course_id
where coursepage.course_id is null
