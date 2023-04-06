select
    b2border_unique_uuid as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__b2becommerce_b2border
where b2border_unique_uuid is not null
group by b2border_unique_uuid
having count(*) > 1
