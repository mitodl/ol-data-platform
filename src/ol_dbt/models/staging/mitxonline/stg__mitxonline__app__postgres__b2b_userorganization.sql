with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_userorganization') }}
)

, cleaned as (
    select
        id as userorganization_id,
        user_id,
        organization_id,
        keep_until_seen as userorganization_keep_until_seen,
        is_manager as userorganization_is_manager
    from source
)

select * from cleaned
