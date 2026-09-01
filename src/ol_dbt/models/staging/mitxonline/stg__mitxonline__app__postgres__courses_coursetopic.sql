with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_coursestopic') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (
    select
        id as coursetopic_id
        , name as coursetopic_name
        , parent_id as coursetopic_parent_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as coursetopic_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as coursetopic_updated_on
    from most_recent_source
)

select * from cleaned
