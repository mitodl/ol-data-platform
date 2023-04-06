select
    b2breceipt_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__b2becommerce_b2breceipt
where b2breceipt_id is not null
group by b2breceipt_id
having count(*) > 1
