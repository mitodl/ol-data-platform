with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_basket') }}

)

{{ deduplicate_raw_table(order_by='id' , partition_columns = 'user_id') }}
, renamed as (

    select
        id as basket_id
        , user_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as basket_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as basket_updated_on

    from most_recent_source

)

select * from renamed
