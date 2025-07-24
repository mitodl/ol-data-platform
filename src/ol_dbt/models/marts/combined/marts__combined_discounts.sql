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
    'MITx Online' as platform
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

union all

select 
    'xPro' as platform
    , coupon_code as discount_code
    , coupon_name as discount_name
    , coupon_created_on as discount_created_on
    , payment_transaction
    , case when discount_id is null then 'b2b' else discount_source end as discount_source
    , activated_on as discount_activated_on
    , expires_on as discount_expires_on
    , couponpaymentversion_num_coupon_codes as num_discount_codes
    , couponpaymentversion_max_redemptions as discount_max_redemptions
    , couponpayment_name
    , b2b_contract_number
    , b2breceipt_reference_number
    , redeemed
    , coupons_used_count as discounts_used_count
    , product_readable_id
    , discount_amount as discount_amount_text
    , company_name
    , coupon_type as discount_type
    , b2bcoupon_id
    , coupon_id as discount_id
    , couponpaymentversion_id
from mitxpro_all_coupons