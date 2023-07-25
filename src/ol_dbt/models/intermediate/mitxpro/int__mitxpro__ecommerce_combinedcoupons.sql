with coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, productcouponassignment as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productcouponassignment') }}
)

, ecommerce_couponversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponversion') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, b2b_ecommerce_b2border as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2border') }}
)

, couponredemption as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponredemption') }}
)

, ecommerce_order as (
    select *
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, users_user as (
    select *
    from {{ ref('int__mitxpro__users') }}
)

, ecommerce_line as (
    select *
    from {{ ref('int__mitxpro__ecommerce_line') }}
)

, program_runs as (
    select *
    from {{ ref('int__mitxpro__program_runs') }}
)

, course_runs as (
    select *
    from {{ ref('int__mitxpro__course_runs') }}
)

, courses as (
    select *
    from {{ ref('int__mitxpro__courses') }}
)

, productversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productversion') }}
)

, b2b_ecommerce_b2breceipt as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2breceipt') }}
)

, payment_transactions as (
    select distinct 
        ecommerce_couponversion.coupon_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
    from ecommerce_couponpaymentversion
    inner join ecommerce_couponversion 
        on ecommerce_couponpaymentversion.couponpaymentversion_id = ecommerce_couponversion.couponpaymentversion_id
    where ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction is not null
)


select 
    productcouponassignment.productcouponassignment_email as coupon_email
    , coupon.coupon_code
    , coupon.coupon_id
    , coupon.coupon_created_on
    , coupon.couponpayment_name
    , null as b2border_contract_number
    , productcouponassignment.product_id
    , payment_transactions.couponpaymentversion_payment_transaction 
    , null as req_reference_number
    , program_runs.programrun_readable_id
    , program_runs.program_title
    , course_runs.courserun_readable_id
    , courses.course_title
    , productversion.productversion_readable_id
    , 'regular coupon' as combinedcoupon_table_source
    , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
    , case when (count(couponredemption.couponredemption_id) >= 1) then 'true' else 'false' end as coupon_redeemed
    , array_join(array_distinct(array_agg(users_user.user_email)), ', ') as combined_user_emails
    , array_join(array_distinct(array_agg(users_user.user_address_country)), ', ') as user_address_countries
    , array_join(array_distinct(array_agg(ecommerce_order.order_id)), ', ') as order_ids
    , array_join(array_distinct(array_agg(ecommerce_order.order_state)), ', ') as combined_order_state
from coupon
left join productcouponassignment
    on productcouponassignment.coupon_id = coupon.coupon_id
left join ecommerce_couponversion 
    on coupon.coupon_id = ecommerce_couponversion.coupon_id
left join ecommerce_couponpaymentversion 
    on ecommerce_couponpaymentversion.couponpaymentversion_id = ecommerce_couponversion.couponpaymentversion_id
left join couponredemption
    on ecommerce_couponversion.couponversion_id = couponredemption.couponversion_id
left join ecommerce_order
    on ecommerce_order.order_id = couponredemption.order_id
left join users_user    
    on ecommerce_order.order_purchaser_user_id = users_user.user_id
left join payment_transactions
    on coupon.coupon_id = payment_transactions.coupon_id
left join ecommerce_line 
    on ecommerce_line.order_id = ecommerce_order.order_id
left join program_runs
    on ecommerce_line.programrun_id = program_runs.programrun_id
left join course_runs
    on ecommerce_line.courserun_id = course_runs.courserun_id
left join courses
    on course_runs.course_id = courses.course_id
left join productversion
    on productversion.productversion_id = ecommerce_line.productversion_id
group by 
    productcouponassignment.productcouponassignment_email 
    , coupon.coupon_code
    , coupon.coupon_id
    , coupon.coupon_created_on
    , coupon.couponpayment_name
    , productcouponassignment.product_id
    , payment_transactions.couponpaymentversion_payment_transaction 
    , program_runs.programrun_readable_id
    , program_runs.program_title
    , course_runs.courserun_readable_id
    , courses.course_title
    , productversion.productversion_readable_id
    , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount

union distinct

select 
    b2b_ecommerce_b2border.b2border_email as coupon_email
    , coupon.coupon_code
    , coupon.coupon_id
    , coupon.coupon_created_on
    , coupon.couponpayment_name
    , b2b_ecommerce_b2border.b2border_contract_number
    , b2b_ecommerce_b2border.product_id
    , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction 
    , replace(
        cast(
            json_extract(b2b_ecommerce_b2breceipt.b2breceipt_data, '$.req_reference_number'
            )
            as varchar
        ), '"', ''
    ) as req_reference_number
    , program_runs.programrun_readable_id
    , program_runs.program_title
    , course_runs.courserun_readable_id
    , courses.course_title
    , productversion.productversion_readable_id
    , 'b2b coupon' as combinedcoupon_table_source
    , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
    , case 
        when (count(couponredemption.couponredemption_id) >= 1) 
            then 'true' 
        else 'false' 
    end as coupon_redeemed
    , array_join(array_distinct(array_agg(users_user.user_email)), ', ') as combined_user_emails
    , array_join(array_distinct(array_agg(users_user.user_address_country)), ', ') as user_address_countries
    , array_join(array_distinct(array_agg(ecommerce_order.order_id)), ', ') as order_ids
    , array_join(array_distinct(array_agg(ecommerce_order.order_state)), ', ') as combined_order_state
from b2b_ecommerce_b2border 
left join ecommerce_couponpaymentversion 
    on ecommerce_couponpaymentversion.couponpaymentversion_id = b2b_ecommerce_b2border.couponpaymentversion_id
left join coupon 
    on coupon.coupon_id = b2b_ecommerce_b2border.b2bcoupon_id 
left join ecommerce_couponversion 
    on ecommerce_couponversion.coupon_id = coupon.coupon_id
left join couponredemption 
    on couponredemption.couponversion_id = ecommerce_couponversion.couponversion_id
left join ecommerce_order 
    on ecommerce_order.order_id = couponredemption.order_id
left join users_user    
    on ecommerce_order.order_purchaser_user_id = users_user.user_id
left join b2b_ecommerce_b2breceipt 
    on b2b_ecommerce_b2breceipt.b2border_id = b2b_ecommerce_b2border.b2border_id
left join ecommerce_line 
    on ecommerce_line.order_id = ecommerce_order.order_id
left join program_runs
    on ecommerce_line.programrun_id = program_runs.programrun_id
left join course_runs
    on ecommerce_line.courserun_id = course_runs.courserun_id
left join courses
    on course_runs.course_id = courses.course_id
left join productversion
    on productversion.productversion_id = b2b_ecommerce_b2border.productversion_id
group by 
    b2b_ecommerce_b2border.b2border_email 
    , coupon.coupon_code
    , coupon.coupon_id
    , coupon.coupon_created_on
    , coupon.couponpayment_name
    , b2b_ecommerce_b2border.b2border_contract_number
    , b2b_ecommerce_b2border.product_id
    , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction 
    , replace(
        cast(
            json_extract(b2b_ecommerce_b2breceipt.b2breceipt_data, '$.req_reference_number')
            as varchar
        ), '"', ''
    ) 
    , program_runs.programrun_readable_id
    , program_runs.program_title
    , course_runs.courserun_readable_id
    , courses.course_title
    , productversion.productversion_readable_id
    , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount