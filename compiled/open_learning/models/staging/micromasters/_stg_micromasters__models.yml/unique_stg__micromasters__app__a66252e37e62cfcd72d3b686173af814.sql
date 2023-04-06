select
    couponinvoice_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__ecommerce_couponinvoice
where couponinvoice_id is not null
group by couponinvoice_id
having count(*) > 1
