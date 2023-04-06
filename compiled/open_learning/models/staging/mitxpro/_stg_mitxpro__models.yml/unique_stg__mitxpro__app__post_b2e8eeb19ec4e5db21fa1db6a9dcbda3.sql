select
    receipt_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_receipt
where receipt_id is not null
group by receipt_id
having count(*) > 1
