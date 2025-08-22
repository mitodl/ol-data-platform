with coursepage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursepage') }}
)

, coursepage_sorted as (
    select
        *
        , row_number() over (partition by course_id order by _airbyte_extracted_at desc) as row_num
    from coursepage
)

, most_recent_coursepage as (
    select * from coursepage_sorted
    where row_num = 1
)

, externalcoursepage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalcoursepage') }}
)

, externalcoursepage_sorted as (
    select
        *
        , row_number() over (partition by course_id order by _airbyte_extracted_at desc) as row_num
    from externalcoursepage
)

, most_recent_externalcoursepage as (
    select * from externalcoursepage_sorted
    where row_num = 1
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
from most_recent_coursepage
union all
select
    most_recent_externalcoursepage.page_ptr_id as wagtail_page_id
    , most_recent_externalcoursepage.description as cms_coursepage_description
    , most_recent_externalcoursepage.course_id
    , most_recent_externalcoursepage.duration as cms_coursepage_duration
    , most_recent_externalcoursepage.format as cms_coursepage_format
    , most_recent_externalcoursepage.subhead as cms_coursepage_subhead
    , most_recent_externalcoursepage.time_commitment as cms_coursepage_time_commitment
    , most_recent_externalcoursepage.catalog_details as cms_coursepage_catalog_details
    , most_recent_externalcoursepage.external_marketing_url as cms_coursepage_external_marketing_url
    , 'cms_externalcoursepage' as cms_coursepage_model

from most_recent_externalcoursepage
-- There are currently courses that has both a course page and an external course page but
-- adding a dedup in case any are added in the future and to match cms_programpage
left join coursepage
    on most_recent_externalcoursepage.course_id = coursepage.course_id
where coursepage.course_id is null
