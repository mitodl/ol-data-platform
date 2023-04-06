select
    basketrunselection_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_basketrunselection
where basketrunselection_id is not null
group by basketrunselection_id
having count(*) > 1
