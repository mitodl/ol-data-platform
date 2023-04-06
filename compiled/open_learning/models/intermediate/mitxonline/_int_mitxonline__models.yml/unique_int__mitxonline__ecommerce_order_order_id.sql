select
    order_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__ecommerce_order
where order_id is not null
group by order_id
having count(*) > 1
