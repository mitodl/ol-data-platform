with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_product') }}

)

, renamed as (

    select
        id as product_id
        , price as product_price
        , is_active as product_is_active
        , object_id as product_object_id
        , description as product_description
        , content_type_id as contenttype_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as product_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as product_updated_on
    from source
)

select * from renamed
