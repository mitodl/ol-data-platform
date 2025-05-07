with coursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursepage_topics') }}
)

, externalcoursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalcoursepage_topics') }}
)

select
    coursepage_id as wagtail_page_id
    , coursetopic_id
from coursepage_topics
union all
select
    externalcoursepage_id as wagtail_page_id
    , coursetopic_id
from externalcoursepage_topics
