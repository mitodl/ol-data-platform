with b2borders as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__b2becommerce_b2border') }}
)

, productversions as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_productversion') }}
)

, products as (
    select *
    from {{ ref('int__mitxpro__ecommerce_product') }}
)

, salesforce_opportunity as (
    select * from {{ ref('int__salesforce__opportunity') }}
)

, couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, salesforce_b2border as (
    select
        b2borders.b2border_id
        , salesforce_opportunity.opportunity_id
    from b2borders
    inner join couponpaymentversion
        on b2borders.couponpaymentversion_id = couponpaymentversion.couponpaymentversion_id
    inner join salesforce_opportunity
        on b2borders.b2border_contract_number like '%' || salesforce_opportunity.opportunity_id
)


select
    b2borders.b2border_id
    , b2borders.b2border_updated_on
    , b2borders.b2border_created_on
    , b2borders.b2border_total_price
    , b2borders.b2border_status
    , b2borders.b2border_per_item_price
    , b2borders.b2border_unique_uuid
    , b2borders.b2border_num_seats
    , b2borders.b2bcoupon_id
    , b2borders.productversion_id
    , b2borders.couponpaymentversion_id
    , b2borders.b2border_contract_number
    , b2borders.b2border_discount
    , b2borders.programrun_id
    , b2borders.b2border_email
    , products.product_id
    , products.courserun_id
    , products.program_id
    , products.product_type
    , salesforce_b2border.opportunity_id as salesforce_opportunity_id
from b2borders
inner join productversions on b2borders.productversion_id = productversions.productversion_id
inner join products on productversions.product_id = products.product_id
left join salesforce_b2border on b2borders.b2border_id = salesforce_b2border.b2border_id
