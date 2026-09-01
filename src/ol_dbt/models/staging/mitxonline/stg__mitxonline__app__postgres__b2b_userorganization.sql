with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_userorganization') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (
    select
        id as userorganization_id,
        user_id,
        organization_id,
        keep_until_seen as userorganization_keep_until_seen,
        is_manager as userorganization_is_manager
    from most_recent_source
)

select * from cleaned
