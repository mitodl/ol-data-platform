with programpage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_programpage') }}
)

, externalprogrampage as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalprogrampage') }}
)


select
    page_ptr_id as wagtail_page_id
    , description as cms_programpage_description
    , program_id
    , duration as cms_programpage_duration
    , subhead as cms_programpage_subhead
    , time_commitment as cms_programpage_time_commitment
    , catalog_details as cms_programpage_catalog_details
    , external_marketing_url as cms_programpage_external_marketing_url
    , featured as cms_programpage_is_featured
    , 'cms_programpage' as cms_programpage_model

from programpage
union all
select
    externalprogrampage.page_ptr_id as wagtail_page_id
    , externalprogrampage.description as cms_cprogrampage_description
    , externalprogrampage.program_id
    , externalprogrampage.duration as cms_programpage_duration
    , externalprogrampage.subhead as cms_programpage_subhead
    , externalprogrampage.time_commitment as cms_programpage_time_commitment
    , externalprogrampage.catalog_details as cms_programpage_catalog_details
    , externalprogrampage.external_marketing_url as cms_programpage_external_marketing_url
    , externalprogrampage.featured as cms_programpage_is_featured
    , 'cms_externalprogrampage' as cms_programpage_model
from externalprogrampage
-- There is one program that has both a program page and an external program page. The external program page is not live
left join programpage
    on externalprogrampage.program_id = programpage.program_id
where programpage.program_id is null
