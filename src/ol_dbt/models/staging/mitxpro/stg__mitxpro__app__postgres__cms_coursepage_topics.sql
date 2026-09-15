with coursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursepage_topics') }}
)

{{ deduplicate_raw_table(
    raw_table='raw__xpro__app__postgres__cms_coursepage_topics'
    , partition_columns='coursepage_id, coursetopic_id'
    , source_cte='coursepage_topics'
) }}

, externalcoursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalcoursepage_topics') }}
)

{{ deduplicate_raw_table(
    raw_table='raw__xpro__app__postgres__cms_externalcoursepage_topics'
    , partition_columns='externalcoursepage_id, coursetopic_id'
    , source_cte='externalcoursepage_topics'
) }}

select
    coursepage_id as wagtail_page_id
    , coursetopic_id
from most_recent_coursepage_topics
union all
select
    externalcoursepage_id as wagtail_page_id
    , coursetopic_id
from most_recent_externalcoursepage_topics
