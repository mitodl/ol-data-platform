with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__ecommerce_productversion") }}

    ),
    renamed as (

        select
            id as productversion_id,
            text_id as productversion_readable_id,
            cast(price as decimal(38, 2)) as productversion_price,
            description as productversion_description,
            product_id,
            requires_enrollment_code as productversion_requires_enrollment_code,
            {{ cast_timestamp_to_iso8601("updated_on") }} as productversion_updated_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as productversion_created_on
        from source

    )

select *
from renamed
