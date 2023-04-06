select
    contenttype_full_name as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__django_contenttype
where contenttype_full_name is not null
group by contenttype_full_name
having count(*) > 1
