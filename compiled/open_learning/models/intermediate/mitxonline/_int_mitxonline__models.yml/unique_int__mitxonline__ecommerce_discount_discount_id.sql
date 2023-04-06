select
    discount_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__ecommerce_discount
where discount_id is not null
group by discount_id
having count(*) > 1
