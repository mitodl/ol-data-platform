-- MITx Online Course Blocked country Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_blockedcountry') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_blockedcountry', partition_columns='id') }}

, cleaned as (
    select
        id as blockedcountry_id
        , country as blockedcountry_code
        , course_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as blockedcountry_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as blockedcountry_updated_on
    from most_recent_source
)

select * from cleaned
