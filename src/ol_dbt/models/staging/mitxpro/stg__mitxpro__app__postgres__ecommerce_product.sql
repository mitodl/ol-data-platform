with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__ecommerce_product") }}),
    renamed as (

        select
            id as product_id,
            is_active as product_is_active,
            object_id as product_object_id,
            is_private as product_is_private,
            content_type_id as contenttype_id,
            {{ cast_timestamp_to_iso8601("updated_on") }} as product_updated_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as product_created_on
        from source

    )

select *
from renamed
