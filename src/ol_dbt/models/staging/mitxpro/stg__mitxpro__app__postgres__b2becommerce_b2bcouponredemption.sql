with
    source as (

        select *
        from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__b2b_ecommerce_b2bcouponredemption") }}

    ),
    renamed as (

        select
            id as b2bcouponredemption_id,
            order_id as b2border_id,
            coupon_id as b2bcoupon_id,
            {{ cast_timestamp_to_iso8601("updated_on") }} as b2bcouponredemption_updated_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as b2bcouponredemption_created_on
        from source
    )

select *
from renamed
