with all_values as (

    select
        order_state as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__ecommerce_order
    group by order_state

)

select *
from all_values
where value_field not in (
    'pending', 'fulfilled', 'canceled', 'declined', 'errored', 'refunded', 'review', 'partially_refunded'
)
