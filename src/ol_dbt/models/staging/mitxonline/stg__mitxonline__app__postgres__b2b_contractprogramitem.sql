with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_contractprogramitem') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__b2b_contractprogramitem', partition_columns='id') }}

, cleaned as (
    select
        id as contractprogramitem_id,
        contract_id,
        program_id,
        sort_order as contractprogramitem_sort_order
    from most_recent_source
)

select * from cleaned
