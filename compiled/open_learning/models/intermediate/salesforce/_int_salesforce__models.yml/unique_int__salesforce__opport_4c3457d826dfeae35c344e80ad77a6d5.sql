select
    opportunitylineitem_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__salesforce__opportunitylineitem
where opportunitylineitem_id is not null
group by opportunitylineitem_id
having count(*) > 1
