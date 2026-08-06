with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_linerunselection') }}

)

{{ deduplicate_raw_table(order_by='updated_on', partition_columns='line_id, run_id') }}
, renamed as (

    select
        id as linerunselection_id
        , line_id
        , run_id as courserun_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as linerunselection_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as linerunselection_updated_on
    from most_recent_source

)

select * from renamed
