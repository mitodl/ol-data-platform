select
    version_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__reversion_version
where version_id is not null
group by version_id
having count(*) > 1
