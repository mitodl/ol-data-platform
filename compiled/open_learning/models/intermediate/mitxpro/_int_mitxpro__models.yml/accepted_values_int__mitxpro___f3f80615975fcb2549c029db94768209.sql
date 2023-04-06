with all_values as (

    select
        couponpaymentversion_discount_source as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxpro__ecommerce_couponpaymentversion
    group by couponpaymentversion_discount_source

)

select *
from all_values
where value_field not in (
    'credit_card', 'purchase_order', 'marketing', 'sales', 'staff', ''
)
