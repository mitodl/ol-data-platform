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

, reg_coupon_summary as (
    select
        coupon.coupon_id
        , case when (count(couponredemption.couponredemption_id) >= 1) then 'true' else 'false' end as coupon_redeemed
        , array_join(array_distinct(array_agg(users_user.user_email)), ', ') as combined_user_emails
        , array_join(array_distinct(array_agg(users_user.user_address_country)), ', ') as user_address_countries
        , array_join(array_distinct(array_agg(ecommerce_order.order_id)), ', ') as order_ids
        , array_join(array_distinct(array_agg(ecommerce_order.order_state)), ', ') as combined_order_state
    from coupon
    left join ecommerce_couponversion
        on coupon.coupon_id = ecommerce_couponversion.coupon_id
    left join couponredemption
        on ecommerce_couponversion.couponversion_id = couponredemption.couponversion_id
    left join ecommerce_order
        on ecommerce_order.order_id = couponredemption.order_id
    left join users_user
        on ecommerce_order.order_purchaser_user_id = users_user.user_id
    group by
        coupon.coupon_id
)

, reg_coupon_fields as (
    select
        productcouponassignment.productcouponassignment_email as coupon_email
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
    from coupon
    inner join productcouponassignment
        on productcouponassignment.coupon_id = coupon.coupon_id
    left join ecommerce_couponversion
        on coupon.coupon_id = ecommerce_couponversion.coupon_id
    left join ecommerce_couponpaymentversion
        on ecommerce_couponpaymentversion.couponpaymentversion_id = ecommerce_couponversion.couponpaymentversion_id
    left join couponredemption
        on ecommerce_couponversion.couponversion_id = couponredemption.couponversion_id
    left join ecommerce_order
        on ecommerce_order.order_id = couponredemption.order_id
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
)

, b2b_coupon_summary as (
    select
        coupon.coupon_id
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
    inner join ecommerce_couponpaymentversion
        on ecommerce_couponpaymentversion.couponpaymentversion_id = b2b_ecommerce_b2border.couponpaymentversion_id
    inner join ecommerce_couponversion
        on ecommerce_couponversion.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
    inner join coupon
        on coupon.coupon_id = ecommerce_couponversion.coupon_id
    left join couponredemption
        on couponredemption.couponversion_id = ecommerce_couponversion.couponversion_id
    left join ecommerce_order
        on ecommerce_order.order_id = couponredemption.order_id
    left join users_user
        on ecommerce_order.order_purchaser_user_id = users_user.user_id
    group by
        coupon.coupon_id
)

, b2b_coupon_fields as (
    select
        b2b_ecommerce_b2border.b2border_email
        , coupon.coupon_code
        , coupon.coupon_id
        , coupon.coupon_created_on
        , coupon.couponpayment_name
        , b2b_ecommerce_b2border.b2border_contract_number
        , b2b_ecommerce_b2border.product_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
        , program_runs.programrun_readable_id
        , program_runs.program_title
        , course_runs.courserun_readable_id
        , courses.course_title
        , productversion.productversion_readable_id
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
        , replace(
            cast(
                json_extract(b2b_ecommerce_b2breceipt.b2breceipt_data, '$.req_reference_number'
                )
                as varchar
            ), '"', ''
        ) as req_reference_number
    from b2b_ecommerce_b2border
    inner join ecommerce_couponpaymentversion
        on ecommerce_couponpaymentversion.couponpaymentversion_id = b2b_ecommerce_b2border.couponpaymentversion_id
    inner join ecommerce_couponversion
        on ecommerce_couponversion.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
    inner join coupon
        on coupon.coupon_id = ecommerce_couponversion.coupon_id
    left join couponredemption
        on couponredemption.couponversion_id = ecommerce_couponversion.couponversion_id
    left join ecommerce_order
        on ecommerce_order.order_id = couponredemption.order_id
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
        , program_runs.programrun_readable_id
        , program_runs.program_title
        , course_runs.courserun_readable_id
        , courses.course_title
        , productversion.productversion_readable_id
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
        , replace(
            cast(
                json_extract(b2b_ecommerce_b2breceipt.b2breceipt_data, '$.req_reference_number')
                as varchar
            ), '"', ''
        )
)


select
    reg_coupon_fields.coupon_email
    , reg_coupon_fields.coupon_code
    , reg_coupon_fields.coupon_id
    , reg_coupon_fields.coupon_created_on
    , reg_coupon_fields.couponpayment_name
    , null as b2border_contract_number
    , reg_coupon_fields.product_id
    , reg_coupon_fields.couponpaymentversion_payment_transaction
    , null as req_reference_number
    , reg_coupon_fields.programrun_readable_id
    , reg_coupon_fields.program_title
    , reg_coupon_fields.courserun_readable_id
    , reg_coupon_fields.course_title
    , reg_coupon_fields.productversion_readable_id
    , 'regular coupon' as combinedcoupon_table_source
    , reg_coupon_fields.couponpaymentversion_coupon_type
    , reg_coupon_fields.couponpaymentversion_discount_amount
    , reg_coupon_summary.coupon_redeemed
    , reg_coupon_summary.combined_user_emails
    , reg_coupon_summary.user_address_countries
    , reg_coupon_summary.order_ids
    , reg_coupon_summary.combined_order_state
from reg_coupon_fields
inner join reg_coupon_summary
    on reg_coupon_fields.coupon_id = reg_coupon_summary.coupon_id

union distinct

select
    b2b_coupon_fields.b2border_email as coupon_email
    , b2b_coupon_fields.coupon_code
    , b2b_coupon_fields.coupon_id
    , b2b_coupon_fields.coupon_created_on
    , b2b_coupon_fields.couponpayment_name
    , b2b_coupon_fields.b2border_contract_number
    , b2b_coupon_fields.product_id
    , b2b_coupon_fields.couponpaymentversion_payment_transaction
    , b2b_coupon_fields.req_reference_number
    , b2b_coupon_fields.programrun_readable_id
    , b2b_coupon_fields.program_title
    , b2b_coupon_fields.courserun_readable_id
    , b2b_coupon_fields.course_title
    , b2b_coupon_fields.productversion_readable_id
    , 'b2b coupon' as combinedcoupon_table_source
    , b2b_coupon_fields.couponpaymentversion_coupon_type
    , b2b_coupon_fields.couponpaymentversion_discount_amount
    , b2b_coupon_summary.coupon_redeemed
    , b2b_coupon_summary.combined_user_emails
    , b2b_coupon_summary.user_address_countries
    , b2b_coupon_summary.order_ids
    , b2b_coupon_summary.combined_order_state
from b2b_coupon_fields
inner join b2b_coupon_summary
    on b2b_coupon_fields.coupon_id = b2b_coupon_summary.coupon_id
