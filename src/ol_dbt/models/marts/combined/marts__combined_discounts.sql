with mitxonline_discount as (
    select * from {{ ref('int__mitxonline__ecommerce_discount') }}
)

, mitxonline_discountproduct as (
    select * from {{ ref('int__mitxonline__ecommerce_discountproduct') }}
)

, mitxonline_product as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

, mitxonline_order as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxpro_all_coupons as (
    select * from {{ ref('marts__mitxpro_all_coupons') }}
)

, combined_products as (
    select * from {{ ref('marts__combined__products') }}
)

, mitxpro_programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxpro_courserun as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, mitxonline_order_sum as (
    select
        discount_code
        , count(distinct case when order_state = 'fulfilled' then order_id end) as coupons_used_count
    from mitxonline_order
    group by discount_code
)

, mitxonline_product_joins as (
    select
        mitxonline_discountproduct.discount_id
        , coalesce(
            mitxonline_product.courserun_readable_id
            , mitxonline_product.program_readable_id
        ) as product_readable_id
    from mitxonline_discountproduct
    left join mitxonline_product
        on mitxonline_discountproduct.product_id = mitxonline_product.product_id
    group by
        mitxonline_discountproduct.discount_id
        , coalesce(
            mitxonline_product.courserun_readable_id
            , mitxonline_product.program_readable_id
        )
)

select
    '{{ var("mitxonline") }}' as platform
    , mitxonline_discount.discount_code
    , null as discount_name
    , mitxonline_discount.discount_created_on
    , null as payment_transaction
    , mitxonline_discount.discount_source
    , mitxonline_discount.discount_activated_on
    , mitxonline_discount.discount_expires_on
    , null as num_discount_codes
    , mitxonline_discount.discount_max_redemptions
    , null as couponpayment_name
    , null as b2b_contract_number
    , null as b2breceipt_reference_number
    , coalesce(case when mitxonline_order_sum.discount_code is not null then true end, false) as redeemed
    , mitxonline_order_sum.coupons_used_count as discounts_used_count
    , mitxonline_product_joins.product_readable_id
    , combined_products.product_name
    , case
        when discount_type = 'percent-off'
            then concat(format('%.2f', mitxonline_discount.discount_amount), '%')
        when discount_type = 'dollars-off'
            then concat('$', format('%.2f', mitxonline_discount.discount_amount))
        when discount_type = 'fixed-price'
            then concat('Fixed Price: ', format('%.2f', mitxonline_discount.discount_amount))
    end as discount_amount_text
    , null as company_name
    , null as discount_type
    , null as b2bcoupon_id
    , mitxonline_discount.discount_id
    , null as couponpaymentversion_id
from mitxonline_discount
left join mitxonline_product_joins
    on mitxonline_discount.discount_id = mitxonline_product_joins.discount_id
left join mitxonline_order_sum
    on mitxonline_discount.discount_code = mitxonline_order_sum.discount_code
left join combined_products
    on mitxonline_product_joins.product_readable_id = combined_products.product_readable_id
    and combined_products.platform = '{{ var("mitxonline") }}'

union all

select
    '{{ var("mitxpro") }}' as platform
    , mitxpro_all_coupons.coupon_code as discount_code
    , mitxpro_all_coupons.coupon_name as discount_name
    , mitxpro_all_coupons.coupon_created_on as discount_created_on
    , mitxpro_all_coupons.payment_transaction
    , case when mitxpro_all_coupons.coupon_id is null then 'b2b' else mitxpro_all_coupons.discount_source
    end as discount_source
    , mitxpro_all_coupons.activated_on as discount_activated_on
    , mitxpro_all_coupons.expires_on as discount_expires_on
    , mitxpro_all_coupons.couponpaymentversion_num_coupon_codes as num_discount_codes
    , mitxpro_all_coupons.couponpaymentversion_max_redemptions as discount_max_redemptions
    , mitxpro_all_coupons.couponpayment_name
    , mitxpro_all_coupons.b2border_contract_number as b2b_contract_number
    , mitxpro_all_coupons.b2breceipt_reference_number
    , mitxpro_all_coupons.redeemed
    , mitxpro_all_coupons.coupons_used_count as discounts_used_count
    , mitxpro_all_coupons.product_readable_id
    , coalesce(combined_products.product_name, mitxpro_programs.program_title, mitxpro_courserun.courserun_title)
        as product_name
    , mitxpro_all_coupons.discount_amount as discount_amount_text
    , mitxpro_all_coupons.company_name
    , mitxpro_all_coupons.coupon_type as discount_type
    , mitxpro_all_coupons.b2bcoupon_id
    , mitxpro_all_coupons.coupon_id as discount_id
    , mitxpro_all_coupons.couponpaymentversion_id
from mitxpro_all_coupons
left join combined_products
    on mitxpro_all_coupons.product_readable_id = combined_products.product_readable_id
    and combined_products.platform = '{{ var("mitxpro") }}'
left join mitxpro_programs
    on mitxpro_all_coupons.product_readable_id = mitxpro_programs.program_readable_id
left join mitxpro_courserun
    on mitxpro_all_coupons.product_readable_id = mitxpro_courserun.courserun_readable_id