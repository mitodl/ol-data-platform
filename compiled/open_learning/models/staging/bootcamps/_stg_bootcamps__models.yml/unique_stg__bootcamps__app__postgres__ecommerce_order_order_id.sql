select
    order_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__bootcamps__app__postgres__ecommerce_order
where order_id is not null
group by order_id
having count(*) > 1
