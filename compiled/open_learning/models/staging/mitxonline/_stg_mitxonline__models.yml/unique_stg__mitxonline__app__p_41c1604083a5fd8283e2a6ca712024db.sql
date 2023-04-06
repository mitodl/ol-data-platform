select
    revision_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__reversion_revision
where revision_id is not null
group by revision_id
having count(*) > 1
