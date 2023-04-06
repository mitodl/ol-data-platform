select
    transaction_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_transaction
where transaction_id is not null
group by transaction_id
having count(*) > 1
