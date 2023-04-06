select
    company_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_company
where company_id is not null
group by company_id
having count(*) > 1
