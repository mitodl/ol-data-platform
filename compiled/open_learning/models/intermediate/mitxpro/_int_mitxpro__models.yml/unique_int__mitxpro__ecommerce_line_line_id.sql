select
    line_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_line
where line_id is not null
group by line_id
having count(*) > 1
