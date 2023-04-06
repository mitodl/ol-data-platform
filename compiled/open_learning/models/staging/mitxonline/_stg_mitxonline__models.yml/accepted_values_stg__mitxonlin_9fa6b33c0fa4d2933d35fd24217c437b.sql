with all_values as (

    select
        order_state as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_order
    group by order_state

)

select *
from all_values
where value_field not in (
    'pending', 'fulfilled', 'canceled', 'declined', 'errored', 'refunded', 'review', 'partially_refunded'
)
