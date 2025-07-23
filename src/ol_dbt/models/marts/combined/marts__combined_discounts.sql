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

select 
    'MITx Online' as platform
    , a.discount_code
    , null as discount_name
    , a.discount_created_on
    , null as payment_transaction
    , a.discount_source
    , a.discount_activated_on
    , a.discount_expires_on
    , null as num_discount_codes
    , a.discount_max_redemptions
    , null as couponpayment_name
    , null as b2border_contract_number
    , null as b2breceipt_reference_number
    , d.coupons_used_count as discounts_used_count
    , coalesce(
        c.courserun_readable_id
        , c.program_readable_id
    ) as product_readable_id
    , coalesce(case when d.discount_code is not null then true end, false) as redeemed
    , case 
        when discount_type = 'percent-off' then cast(a.discount_amount as varchar) || '%'
        when discount_type = 'dollars-off' then '$' || cast(a.discount_amount as varchar) || ' off'
        when discount_type = 'fixed-price' then '$' || cast(a.discount_amount as varchar) || ' fixed price' 
    end 
        as discount_amount_text
    , null as company_name
    , null as discount_type
    , null as b2bcoupon_id
    , a.discount_id
from mitxonline_discount as a
left join mitxonline_discountproduct as b
    on a.discount_id = b.discount_id
left join mitxonline_product as c
    on b.product_id = c.product_id
left join mitxonline_order_sum as d
    on a.discount_code = d.discount_code

union all

select 
    'xPro' as platform
    , coupon_code as discount_code
    , coupon_name as discount_name
    , coupon_created_on as discount_created_on
    , payment_transaction
    , discount_source
    , activated_on as discount_activated_on
    , expires_on as discount_expires_on
    , couponpaymentversion_num_coupon_codes as num_discount_codes
    , couponpaymentversion_max_redemptions as discount_max_redemptions
    , couponpayment_name
    , b2border_contract_number
    , b2breceipt_reference_number
    , coupons_used_count as discounts_used_count
    , product_readable_id
    , redeemed
    , discount_amount as discount_amount_text
    , company_name
    , coupon_type as discount_type
    , b2bcoupon_id
    , coupon_id as discount_id
from mitxpro_all_coupons