with coursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursepage_topics') }}
)

, coursepage_topics_deduped as (
    select
        *
        , row_number() over (partition by coursepage_id, coursetopic_id order by _airbyte_extracted_at desc) as row_num
    from coursepage_topics
)

, externalcoursepage_topics as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_externalcoursepage_topics') }}
)

, externalcoursepage_topics_deduped as (
    select
        *
        , row_number() over (partition by externalcoursepage_id, coursetopic_id order by _airbyte_extracted_at desc) as row_num
    from externalcoursepage_topics
)

select
    coursepage_id as wagtail_page_id
    , coursetopic_id
from coursepage_topics_deduped
where row_num = 1
union all
select
    externalcoursepage_id as wagtail_page_id
    , coursetopic_id
from externalcoursepage_topics_deduped
where row_num = 1
