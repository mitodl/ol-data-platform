-- MITxPro Course Topic Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_coursetopic') }}
)

, cleaned as (
    select
        id as coursetopic_id
        , name as coursetopic_name
        , {{ cast_timestamp_to_iso8601('created_on') }} as coursetopic_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as coursetopic_updated_on
    from source
)

select * from cleaned
