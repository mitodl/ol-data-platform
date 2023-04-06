with all_values as (

    select
        discount_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__ecommerce_discount
    group by discount_type

)

select *
from all_values
where value_field not in (
    'percent-off', 'dollars-off', 'fixed-price'
)
