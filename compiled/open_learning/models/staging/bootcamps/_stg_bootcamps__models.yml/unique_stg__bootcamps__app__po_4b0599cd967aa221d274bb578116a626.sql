select
    orderaudit_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__bootcamps__app__postgres__ecommerce_orderaudit
where orderaudit_id is not null
group by orderaudit_id
having count(*) > 1
