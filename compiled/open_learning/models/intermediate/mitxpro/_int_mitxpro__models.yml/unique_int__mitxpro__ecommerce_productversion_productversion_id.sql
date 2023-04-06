select
    productversion_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_productversion
where productversion_id is not null
group by productversion_id
having count(*) > 1
