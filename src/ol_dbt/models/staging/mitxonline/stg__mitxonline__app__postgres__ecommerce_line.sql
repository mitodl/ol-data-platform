with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_line') }}

)

, renamed as (

    select
        id as line_id
        , order_id
        , created_on as line_created_on
        , updated_on as line_updated_on
        , product_version_id
        , purchased_object_id as product_object_id
        , purchased_content_type_id as contenttype_id
    from source

)

select * from renamed
