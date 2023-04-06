with all_values as (

    select
        transaction_type as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_transaction
    group by transaction_type

)

select *
from all_values
where value_field not in (
    'refund', 'payment'
)
