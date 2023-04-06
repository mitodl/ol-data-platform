select
    b2borderaudit_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2borderaudit
where b2borderaudit_id is not null
group by b2borderaudit_id
having count(*) > 1
