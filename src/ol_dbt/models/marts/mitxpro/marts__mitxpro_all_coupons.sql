with allcoupons as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

, allorders as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, ecommerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, ecommerce_company as (
    select *
    from {{ ref('int__mitxpro__ecommerce_company') }}
)

, ecommerce_line as (
    select *
    from {{ ref('int__mitxpro__ecommerce_line') }}
)

, mitxpro__programruns as (
    select *
    from {{ ref('int__mitxpro__program_runs') }}
)

, ecommerce_order as (
    select *
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, redeemed_coupons as (
    select coupon_id
    from allorders
    where redeemed = true
    group by coupon_id
)

, redeemed_b2b_coupons as (
    select
        b2bcoupon_id
        , b2border_contract_number
    from allorders
    where
        redeemed = true
        and coupon_id is null
    group by
        b2bcoupon_id
        , b2border_contract_number
)

, coupons_used_by_name as (
    select
        ecommerce_coupon.couponpayment_name
        , count(distinct ecommerce_order.order_id) as coupons_used_count
    from ecommerce_order
    inner join ecommerce_coupon
        on ecommerce_order.coupon_id = ecommerce_coupon.coupon_id
    where ecommerce_order.order_state = 'fulfilled'
    group by ecommerce_coupon.couponpayment_name
)

, pull_product_readable_id as (
    select
        allorders.coupon_id
        , allorders.b2bcoupon_id
        , ecommerce_couponpaymentversion.couponpaymentversion_id
        , allorders.b2border_contract_number
        , coalesce(
            allorders.courserun_readable_id
            , mitxpro__programruns.programrun_readable_id
            , allorders.program_readable_id
        ) as product_readable_id
    from allorders
    left join ecommerce_line
        on allorders.line_id = ecommerce_line.line_id
    left join mitxpro__programruns
        on ecommerce_line.programrun_id = mitxpro__programruns.programrun_id
    left join ecommerce_couponpaymentversion
        on
            allorders.couponpaymentversion_payment_transaction
            = ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
    where allorders.order_id is not null
    group by
        allorders.coupon_id
        , allorders.b2bcoupon_id
        , ecommerce_couponpaymentversion.couponpaymentversion_id
        , allorders.b2border_contract_number
        , coalesce(
            allorders.courserun_readable_id
            , mitxpro__programruns.programrun_readable_id
            , allorders.program_readable_id
        )
)

select
    allcoupons.coupon_code
    , allcoupons.coupon_name
    , allcoupons.coupon_created_on
    , allcoupons.payment_transaction
    , allcoupons.discount_amount
    , allcoupons.coupon_type
    , allcoupons.discount_source
    , ecommerce_couponpaymentversion.couponpaymentversion_activated_on as activated_on
    , ecommerce_couponpaymentversion.couponpaymentversion_expires_on as expires_on
    , allcoupons.coupon_source_table
    , allcoupons.b2bcoupon_id
    , allcoupons.coupon_id
    , ecommerce_couponpaymentversion.couponpaymentversion_num_coupon_codes
    , ecommerce_couponpaymentversion.couponpaymentversion_max_redemptions
    , ecommerce_couponpaymentversion.couponpayment_name
    , ecommerce_couponpaymentversion.couponpaymentversion_id
    , ecommerce_couponpaymentversion.couponpaymentversion_created_on
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount_text
    , ecommerce_company.company_name
    , redeemed_b2b_coupons.b2border_contract_number
    , pull_product_readable_id.product_readable_id
    , coupons_used_by_name.coupons_used_count
    , case
        when
            redeemed_coupons.coupon_id is not null
            or redeemed_b2b_coupons.b2bcoupon_id is not null
            then true
        when
            redeemed_coupons.coupon_id is null
            and redeemed_b2b_coupons.b2bcoupon_id is null
            then false
    end as redeemed
from allcoupons
left join redeemed_coupons
    on allcoupons.coupon_id = redeemed_coupons.coupon_id
left join redeemed_b2b_coupons
    on allcoupons.b2bcoupon_id = redeemed_b2b_coupons.b2bcoupon_id
left join ecommerce_coupon
    on allcoupons.coupon_id = ecommerce_coupon.coupon_id
left join ecommerce_couponpaymentversion
    on ecommerce_coupon.couponpayment_name = ecommerce_couponpaymentversion.couponpayment_name
left join ecommerce_company
    on ecommerce_couponpaymentversion.company_id = ecommerce_company.company_id
left join coupons_used_by_name
    on ecommerce_couponpaymentversion.couponpayment_name = coupons_used_by_name.couponpayment_name
left join pull_product_readable_id
    on
        (allcoupons.coupon_id is null or allcoupons.coupon_id = pull_product_readable_id.coupon_id)
        and (allcoupons.b2bcoupon_id is null or allcoupons.b2bcoupon_id = pull_product_readable_id.b2bcoupon_id)
        and (
            ecommerce_couponpaymentversion.couponpaymentversion_id is null
            or ecommerce_couponpaymentversion.couponpaymentversion_id = pull_product_readable_id.couponpaymentversion_id
        )
        and (
            redeemed_b2b_coupons.b2border_contract_number is null
            or redeemed_b2b_coupons.b2border_contract_number = pull_product_readable_id.b2border_contract_number
        )
