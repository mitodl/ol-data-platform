with all_values as (

    select
        discount_redemption_type as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_discount
    group by discount_redemption_type

)

select *
from all_values
where value_field not in (
    'one-time', 'one-time-per-user', 'unlimited'
)
