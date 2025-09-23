with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__app__postgres__ecommerce_discountproduct") }}

    ),
    renamed as (

        select
            id as discountproduct_id,
            product_id,
            discount_id,
            {{ cast_timestamp_to_iso8601("created_on") }} as discountproduct_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as discountproduct_updated_on

        from source

    )

select *
from renamed
