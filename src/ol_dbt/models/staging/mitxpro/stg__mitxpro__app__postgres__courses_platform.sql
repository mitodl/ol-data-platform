-- MITxPro Platform Information
with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__courses_platform") }}),
    cleaned as (select id as platform_id, name as platform_name from source)

select *
from cleaned
