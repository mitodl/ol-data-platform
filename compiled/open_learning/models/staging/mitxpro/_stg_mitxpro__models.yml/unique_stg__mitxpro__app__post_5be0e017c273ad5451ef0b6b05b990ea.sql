select
    b2border_unique_uuid as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2border
where b2border_unique_uuid is not null
group by b2border_unique_uuid
having count(*) > 1
