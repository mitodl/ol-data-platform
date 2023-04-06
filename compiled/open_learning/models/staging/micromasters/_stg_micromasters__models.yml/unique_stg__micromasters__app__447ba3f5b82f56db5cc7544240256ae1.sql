select
    order_reference_number as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__ecommerce_order
where order_reference_number is not null
group by order_reference_number
having count(*) > 1
