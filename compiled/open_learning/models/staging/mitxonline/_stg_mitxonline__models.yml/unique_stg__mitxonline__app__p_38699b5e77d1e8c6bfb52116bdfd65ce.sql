select
    coursetopic_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_coursetopic
where coursetopic_id is not null
group by coursetopic_id
having count(*) > 1
