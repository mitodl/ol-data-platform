-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__users_legaladdress') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (

    select
        id as user_address_id
        , country as user_address_country
        , state as user_address_state
        , user_id
    from most_recent_source
)

select * from cleaned
