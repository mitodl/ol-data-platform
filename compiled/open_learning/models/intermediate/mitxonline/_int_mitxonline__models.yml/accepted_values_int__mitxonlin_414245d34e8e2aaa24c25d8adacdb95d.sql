with all_values as (

    select
        product_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__ecommerce_order
    group by product_type

)

select *
from all_values
where value_field not in (
    'course run', 'program run'
)
