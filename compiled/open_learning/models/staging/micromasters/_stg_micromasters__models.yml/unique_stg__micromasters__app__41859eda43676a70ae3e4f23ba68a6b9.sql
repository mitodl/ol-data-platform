select
    contenttype_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__django_contenttype
where contenttype_id is not null
group by contenttype_id
having count(*) > 1
