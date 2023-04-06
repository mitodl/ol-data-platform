select
    transaction_readable_identifier as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__ecommerce_transaction
where transaction_readable_identifier is not null
group by transaction_readable_identifier
having count(*) > 1
