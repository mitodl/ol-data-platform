select
    electiveset_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__courses_electiveset
where electiveset_id is not null
group by electiveset_id
having count(*) > 1
