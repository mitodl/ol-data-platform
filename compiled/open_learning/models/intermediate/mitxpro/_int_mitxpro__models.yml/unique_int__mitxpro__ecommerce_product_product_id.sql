select
    product_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_product
where product_id is not null
group by product_id
having count(*) > 1
