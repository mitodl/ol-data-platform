select
    company_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_company
where company_id is not null
group by company_id
having count(*) > 1
