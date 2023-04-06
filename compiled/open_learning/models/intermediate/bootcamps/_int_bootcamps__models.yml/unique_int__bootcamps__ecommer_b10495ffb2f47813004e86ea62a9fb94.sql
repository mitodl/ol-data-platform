select
    wiretransferreceipt_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__bootcamps__ecommerce_wiretransferreceipt
where wiretransferreceipt_id is not null
group by wiretransferreceipt_id
having count(*) > 1
