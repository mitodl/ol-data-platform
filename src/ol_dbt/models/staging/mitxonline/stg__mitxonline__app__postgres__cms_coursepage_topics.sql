with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_coursepage_topics') }}
)

select
    coursepage_id as wagtail_page_id
    , coursestopic_id as coursetopic_id
from source
