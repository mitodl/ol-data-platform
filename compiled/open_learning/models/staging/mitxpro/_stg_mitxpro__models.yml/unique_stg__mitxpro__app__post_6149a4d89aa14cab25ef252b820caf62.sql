select
    b2border_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2border
where b2border_id is not null
group by b2border_id
having count(*) > 1
