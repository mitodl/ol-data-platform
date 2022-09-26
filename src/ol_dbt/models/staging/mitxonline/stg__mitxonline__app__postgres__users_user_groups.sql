-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__users_user_groups') }}
),

cleaned as (

    select
        id,
        group_id,
        user_id
    from source
)

select * from cleaned