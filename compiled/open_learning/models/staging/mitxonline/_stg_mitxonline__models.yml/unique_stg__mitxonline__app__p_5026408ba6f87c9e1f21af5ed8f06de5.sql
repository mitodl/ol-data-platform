select
    coursetopic_name as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_coursetopic
where coursetopic_name is not null
group by coursetopic_name
having count(*) > 1
