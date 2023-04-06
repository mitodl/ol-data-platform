with all_values as (

    select
        transaction_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__ecommerce_transaction
    group by transaction_type

)

select *
from all_values
where value_field not in (
    'refund', 'payment'
)
