with all_values as (

    select
        coupon_amount_type as value_field
        , count(*) as n_records

    from dev.main_staging.stg__micromasters__app__postgres__ecommerce_coupon
    group by coupon_amount_type

)

select *
from all_values
where value_field not in (
    'fixed-discount', 'fixed-price', 'percent-discount'
)
