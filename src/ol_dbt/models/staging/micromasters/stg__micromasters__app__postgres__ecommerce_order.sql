with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__ecommerce_order") }}

    ),
    renamed as (

        select
            id as order_id,
            status as order_state,
            user_id,
            reference_number as order_reference_number,
            cast(total_price_paid as decimal(38, 2)) as order_total_price_paid,
            {{ cast_timestamp_to_iso8601("created_at") }} as order_created_on,
            {{ cast_timestamp_to_iso8601("modified_at") }} as order_updated_on
        from source

    )

select *
from renamed
